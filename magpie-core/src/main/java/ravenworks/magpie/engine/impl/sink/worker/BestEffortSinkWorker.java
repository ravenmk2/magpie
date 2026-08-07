package ravenworks.magpie.engine.impl.sink.worker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.StreamConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class BestEffortSinkWorker implements SinkWorker {

    private enum State {NORMAL, RETRYING}


    private static final Object POLL_SIGNAL = new Object();
    private static final int EMPTY_POLL_THRESHOLD = 5;

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final RetryMessageStore retryStore;
    private final WorkLoop workLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final int batchSize;

    private volatile Thread loopThread;
    private State state;
    private boolean hasRetryable;
    private int emptyPollCount;

    public BestEffortSinkWorker(@NonNull String name,
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
        this.workLoop = new WorkLoop("snk-" + name, 1_000, this::dispatch);
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.workLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        switch (event) {
            case WorkLoop.Started _ -> onStart();
            case WorkLoop.Idle _ -> onIdle();
            case WorkLoop.PreShutdown _ -> onPreShutdown();
            default -> {
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.consumer.start();
        this.state = State.RETRYING;
        this.workLoop.enqueue(POLL_SIGNAL);
    }

    private void onIdle() {
        this.pollAndProcess();
    }

    private void onPreShutdown() {
        long lastOffset = this.lastOffset.get();
        if (lastOffset >= 0) {
            log.info("[{}] committing offset {} before shutdown", this.name, lastOffset);
            this.consumer.commit(lastOffset);
        }
        try {
            this.consumer.stop();
        } catch (Exception ex) {
            log.warn("[{}] error stopping consumer", this.name, ex);
        }
    }

    private void pollAndProcess() {
        if (this.workLoop.getState() != WorkLoopState.RUNNING) {
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
                        this.state = State.RETRYING;
                        log.info("[{}] entering RETRYING mode", this.name);
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
            this.workLoop.enqueue(POLL_SIGNAL);
        }
    }

    private void processBatch(List<ConsumerRecord> batch) {
        List<SinkResult> results = this.handler.handle(batch).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                SinkWorkerUtils.saveWithRetry(this.retryStore, this.name, result.getRecord(), this.workLoop);
                this.hasRetryable = true;
            }
            this.lastOffset.updateAndGet(o -> Math.max(o, result.getRecord().getOffset()));
        }
    }

    private void pollAndProcessRetrying() {
        try {
            // 只取已到期的重试项；未到期项留在库中等待退避结束
            var entries = this.retryStore.listRetryable(this.name, this.batchSize);
            if (entries.isEmpty()) {
                // 没有到期项不代表存储为空：退避中的消息仍在，hasRetryable 不能复位
                this.hasRetryable = !this.retryStore.list(this.name, 1).isEmpty();
                this.emptyPollCount = 0;
                this.state = State.NORMAL;
                log.info("[{}] exiting RETRYING mode", this.name);
                return;
            }

            List<ConsumerRecord> records = entries.stream()
                    .map(e -> new ConsumerRecord()
                            .setOffset(e.getOffset())
                            .setId(e.getMessageId())
                            .setType(e.getType())
                            .setEventTime(e.getEventTime())
                            .setTopic(e.getTopic())
                            .setTenantId(e.getTenantId())
                            .setBusinessKey(e.getBusinessKey())
                            .setHeaders(e.getHeaders())
                            .setPayload(e.getPayload()))
                    .toList();

            List<SinkResult> results = this.handler.handle(records).join();

            Map<Long, RetryRecord> entryByOffset = new HashMap<>();
            for (var e : entries) {
                entryByOffset.put(e.getOffset(), e);
            }

            boolean allSuccess = true;
            for (var result : results) {
                RetryRecord entry = entryByOffset.get(result.getRecord().getOffset());
                if (entry == null) {
                    continue;
                }
                if (result.getStatus() == SinkStatus.SUCCESS) {
                    this.retryStore.succeeded(entry.getId());
                } else {
                    this.retryStore.failed(entry.getId());
                    allSuccess = false;
                }
            }

            if (!allSuccess) {
                this.state = State.NORMAL;
                log.info("[{}] retry partially failed, exiting RETRYING mode", this.name);
            }
        } catch (Exception e) {
            // 系统性故障（如 DB 瞬断）：停顿后下轮继续，避免忙转
            log.warn("[{}] retry poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            // 异常（如重试状态更新失败）不能中断轮询循环
            this.workLoop.enqueue(POLL_SIGNAL);
        }
    }

}
