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
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
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
    private final EventLoop eventLoop;
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
        this.state = State.RETRYING;
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
                    this.state = State.RETRYING;
                    log.info("[{}] entering RETRYING mode", this.name);
                }
                this.emptyPollCount = 0;
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void processBatch(List<ConsumerRecord> batch) {
        List<SinkResult> results = this.handler.handle(batch).join();
        for (var result : results) {
            this.lastOffset.updateAndGet(o -> Math.max(o, result.getRecord().getOffset()));
            if (result.getStatus() != SinkStatus.SUCCESS) {
                this.retryStore.save(this.name, result.getRecord());
                this.hasRetryable = true;
            }
        }
    }

    private void pollAndProcessRetrying() {
        var entries = this.retryStore.listRetryable(this.name, this.batchSize);
        if (entries.isEmpty()) {
            this.hasRetryable = false;
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            log.info("[{}] exiting RETRYING mode", this.name);
            this.eventLoop.enqueue(POLL_SIGNAL);
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
                this.retryStore.failed(entry.getId(), LocalDateTime.now());
                allSuccess = false;
            }
        }

        if (!allSuccess) {
            this.state = State.NORMAL;
            log.info("[{}] retry partially failed, exiting RETRYING mode", this.name);
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

}
