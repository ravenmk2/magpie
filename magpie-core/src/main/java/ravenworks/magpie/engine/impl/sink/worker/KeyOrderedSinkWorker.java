package ravenworks.magpie.engine.impl.sink.worker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.runtime.EventLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageUtils;
import ravenworks.magpie.engine.api.stream.StreamConsumer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class KeyOrderedSinkWorker implements SinkWorker {

    private enum State {NORMAL, RETRYING}


    private static final Object POLL_SIGNAL = new Object();
    private static final int EMPTY_POLL_THRESHOLD = 5;

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final RetryMessageStore retryStore;
    private final EventLoop eventLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final int batchSize;

    private final Set<String> blockedKeys = new HashSet<>();
    private volatile Thread loopThread;
    private State state;
    private boolean hasRetryable;
    private int emptyPollCount;
    /** 停机时落库失败的最小 offset：停机提交不得越过它（at-least-once 缺口防护） */
    private long firstUnpersistedOffset = Long.MAX_VALUE;

    public KeyOrderedSinkWorker(@NonNull String name,
                                @NonNull StreamConsumer consumer,
                                @NonNull SinkHandler handler,
                                @NonNull CircuitBreaker circuitBreaker,
                                @NonNull RetryMessageStore retryStore,
                                int batchSize) {
        this.name = name;
        this.consumer = consumer;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
        this.retryStore = retryStore;
        this.batchSize = batchSize;
        this.eventLoop = new EventLoop("snk-" + name, 1_000, this::dispatch);
    }

    @Override
    public void start() {
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        switch (event) {
            case EventLoop.Started _ -> onStart();
            case EventLoop.Idle _ -> onIdle();
            case EventLoop.PreShutdown _ -> onPreShutdown();
            default -> {
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.consumer.start();
        this.blockedKeys.addAll(this.retryStore.listKeys(this.name));
        if (!this.blockedKeys.isEmpty()) {
            this.state = State.RETRYING;
            log.info("[{}] entering RETRYING mode, {} blocked keys",
                    this.name, this.blockedKeys.size());
        } else {
            this.state = State.NORMAL;
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void onIdle() {
        this.pollAndProcess();
    }

    private void onPreShutdown() {
        // 提交水位不得越过停机时未能落库的消息：只提交缺口之前，其余等重启重投
        long committable = Math.min(this.lastOffset.get(), this.firstUnpersistedOffset - 1);
        if (this.firstUnpersistedOffset <= this.lastOffset.get()) {
            log.warn("[{}] clamping shutdown commit to {}: message at offset {} was not persisted",
                    this.name, committable, this.firstUnpersistedOffset);
        }
        if (committable >= 0) {
            log.info("[{}] committing offset {} before shutdown", this.name, committable);
            this.consumer.commit(committable);
        }
        try {
            this.consumer.stop();
        } catch (Exception ex) {
            log.warn("[{}] error stopping consumer", this.name, ex);
        }
    }

    private void pollAndProcess() {
        if (this.eventLoop.getState() != EventLoopState.RUNNING) {
            return;
        }
        if (this.circuitBreaker.isOpen()) {
            return;
        }
        if (this.state == State.RETRYING) {
            pollAndProcessRetrying();
        } else {
            pollAndProcessNormal();
        }
    }

    private void pollAndProcessNormal() {
        try {
            var batch = this.consumer.poll(this.batchSize, Duration.ofMillis(50));
            batch = SinkWorkerUtils.filterByOffset(this.name, batch, this.lastOffset.get());
            if (!batch.isEmpty()) {
                processBatch(batch);
                long offset = this.lastOffset.get();
                if (offset >= 0) {
                    this.consumer.commit(offset);
                }
                this.emptyPollCount = 0;
            } else if (this.hasRetryable) {
                this.emptyPollCount++;
                if (this.emptyPollCount >= EMPTY_POLL_THRESHOLD) {
                    if (!this.retryStore.listRetryable(this.name, 1).isEmpty()) {
                        enterRetrying();
                    }
                    this.emptyPollCount = 0;
                }
            }
        } catch (Exception e) {
            // 系统性故障（如 Stream/DB 瞬断）：停顿后下轮继续，避免忙转
            log.warn("[{}] poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            // 异常（如重试落库失败）不能中断轮询循环
            this.eventLoop.enqueue(POLL_SIGNAL);
        }
    }

    private void processBatch(List<ConsumerRecord> batch) {
        var remaining = filterBlocked(batch);
        if (remaining.isEmpty()) {
            return;
        }
        var subBatches = MessageUtils.batchByUniqueKey(remaining, KeyOrderedSinkWorker::keyOf);
        for (var subBatch : subBatches) {
            processSubBatch(subBatch);
        }
    }

    private List<ConsumerRecord> filterBlocked(List<ConsumerRecord> batch) {
        if (this.blockedKeys.isEmpty()) {
            return batch;
        }
        List<ConsumerRecord> remaining = new ArrayList<>();
        for (var record : batch) {
            if (isBlocked(record)) {
                this.saveWithGapTracking(record);
                this.hasRetryable = true;
                this.lastOffset.updateAndGet(o -> Math.max(o, record.getOffset()));
            } else {
                remaining.add(record);
            }
        }
        return remaining;
    }

    private boolean isBlocked(ConsumerRecord record) {
        return this.blockedKeys.contains(keyOf(record));
    }

    /**
     * businessKey 归一化：null 视为 ""。与批量分组及仓储落库（NOT NULL 列）保持一致，
     * 否则 null key 消息既不阻塞也不分流，key 内顺序无从保证。
     */
    private static String keyOf(ConsumerRecord record) {
        return record.getBusinessKey() != null ? record.getBusinessKey() : "";
    }

    /**
     * 落库包装：停机中落库失败时（saveWithRetry 按设计放弃重试并抛出），先记录未能持久化的
     * 最小 offset 再抛出——onPreShutdown 的提交不得越过它，否则该消息既未投递又未落库，
     * 却随水位提交被跳过，永久丢失。正常运行期 saveWithRetry 原地重试不抛出，不影响正常语义。
     */
    private void saveWithGapTracking(ConsumerRecord record) {
        try {
            SinkWorkerUtils.saveWithRetry(this.retryStore, this.name, record, this.eventLoop);
        } catch (RuntimeException e) {
            this.firstUnpersistedOffset = Math.min(this.firstUnpersistedOffset, record.getOffset());
            throw e;
        }
    }

    private void processSubBatch(List<ConsumerRecord> subBatch) {
        List<ConsumerRecord> toSend = new ArrayList<>();
        for (var record : subBatch) {
            if (isBlocked(record)) {
                this.saveWithGapTracking(record);
                this.hasRetryable = true;
                this.lastOffset.updateAndGet(o -> Math.max(o, record.getOffset()));
            } else {
                toSend.add(record);
            }
        }
        if (toSend.isEmpty()) {
            return;
        }
        List<SinkResult> results = this.handler.handle(toSend).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                this.saveWithGapTracking(result.getRecord());
                this.hasRetryable = true;
                this.blockedKeys.add(keyOf(result.getRecord()));
            }
            this.lastOffset.updateAndGet(o -> Math.max(o, result.getRecord().getOffset()));
        }
    }

    private void enterRetrying() {
        this.state = State.RETRYING;
        log.info("[{}] entering RETRYING mode", this.name);
    }

    private void refreshBlockedKeys() {
        var keys = this.retryStore.listKeys(this.name);
        this.blockedKeys.clear();
        this.blockedKeys.addAll(keys);
    }

    private void pollAndProcessRetrying() {
        try {
            // 只取已到期的重试项（按 offset 升序保证 key 内顺序）；未到期项留在库中等待
            var entries = this.retryStore.listRetryable(this.name, this.batchSize);
            if (entries.isEmpty()) {
                // 没有到期项不代表存储为空：退避中的消息仍在，hasRetryable 不能复位
                this.hasRetryable = !this.retryStore.list(this.name, 1).isEmpty();
                refreshBlockedKeys();
                this.emptyPollCount = 0;
                this.state = State.NORMAL;
                log.info("[{}] exiting RETRYING mode, {} blocked keys",
                        this.name, this.blockedKeys.size());
                return;
            }

            Map<Long, RetryRecord> entryByOffset = new HashMap<>();
            List<ConsumerRecord> records = new ArrayList<>();
            for (var e : entries) {
                entryByOffset.put(e.getOffset(), e);
                records.add(new ConsumerRecord()
                        .setOffset(e.getOffset())
                        .setId(e.getMessageId())
                        .setType(e.getType())
                        .setEventTime(e.getEventTime())
                        .setTopic(e.getTopic())
                        .setTenantId(e.getTenantId())
                        .setBusinessKey(e.getBusinessKey())
                        .setHeaders(e.getHeaders())
                        .setPayload(e.getPayload()));
            }

            var subBatches = MessageUtils.batchByUniqueKey(records, KeyOrderedSinkWorker::keyOf);
            int failedCount = 0;
            for (var subBatch : subBatches) {
                List<SinkResult> results = this.handler.handle(subBatch).join();
                for (var result : results) {
                    RetryRecord entry = entryByOffset.get(result.getRecord().getOffset());
                    if (entry == null) {
                        continue;
                    }
                    if (result.getStatus() == SinkStatus.SUCCESS) {
                        this.retryStore.succeeded(entry.getId());
                    } else {
                        this.retryStore.failed(entry.getId());
                        log.warn("[{}] retry failed for {}", this.name, entry.getId());
                        failedCount++;
                    }
                }
                if (failedCount > 0) {
                    break;
                }
            }
            if (failedCount > 0) {
                refreshBlockedKeys();
                this.state = State.NORMAL;
                log.info("[{}] retry batch had {} failure(s), exiting RETRYING mode, {} blocked keys",
                        this.name, failedCount, this.blockedKeys.size());
            }
        } catch (Exception e) {
            // 系统性故障（如 DB 瞬断）：停顿后下轮继续，避免忙转
            log.warn("[{}] retry poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            // 异常（如重试状态更新失败）不能中断轮询循环
            this.eventLoop.enqueue(POLL_SIGNAL);
        }
    }

}
