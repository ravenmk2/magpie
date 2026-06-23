package ravenworks.magpie.engine.sink.common;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.runtime.EventLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.retry.RetryRecord;
import ravenworks.magpie.engine.sink.SinkHandler;
import ravenworks.magpie.engine.sink.SinkResult;
import ravenworks.magpie.engine.sink.SinkStatus;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.MessageUtils;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
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
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void processBatch(List<ConsumerRecord> batch) {
        var remaining = filterBlocked(batch);
        if (remaining.isEmpty()) {
            return;
        }
        var subBatches = MessageUtils.batchByUniqueKey(remaining,
                r -> r.getBusinessKey() != null ? r.getBusinessKey() : "");
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
                this.retryStore.save(this.name, record);
                this.hasRetryable = true;
                this.lastOffset.updateAndGet(o -> Math.max(o, record.getOffset()));
            } else {
                remaining.add(record);
            }
        }
        return remaining;
    }

    private boolean isBlocked(ConsumerRecord record) {
        String key = record.getBusinessKey();
        return key != null && this.blockedKeys.contains(key);
    }

    private void processSubBatch(List<ConsumerRecord> subBatch) {
        List<ConsumerRecord> toSend = new ArrayList<>();
        for (var record : subBatch) {
            if (isBlocked(record)) {
                this.retryStore.save(this.name, record);
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
            this.lastOffset.updateAndGet(o -> Math.max(o, result.getRecord().getOffset()));
            if (result.getStatus() == SinkStatus.SUCCESS) {
                continue;
            }
            this.retryStore.save(this.name, result.getRecord());
            this.hasRetryable = true;
            String key = result.getRecord().getBusinessKey();
            if (key != null) {
                this.blockedKeys.add(key);
            }
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
        var entries = this.retryStore.list(this.name, this.batchSize);
        if (entries.isEmpty()) {
            refreshBlockedKeys();
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            log.info("[{}] exiting RETRYING mode, {} blocked keys",
                    this.name, this.blockedKeys.size());
            this.eventLoop.enqueue(POLL_SIGNAL);
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

        var subBatches = MessageUtils.batchByUniqueKey(records,
                r -> r.getBusinessKey() != null ? r.getBusinessKey() : "");
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
                    this.retryStore.failed(entry.getId(), LocalDateTime.now());
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
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

}
