package ravenworks.magpie.engine.sink.common;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.runtime.EventLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkHandler;
import ravenworks.magpie.engine.sink.SinkResult;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class OrderedSinkWorker implements SinkWorker {

    private static final Object POLL_SIGNAL = new Object();
    private static final int BATCH_SIZE = 100;

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final EventLoop eventLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);

    private volatile Thread loopThread;

    public OrderedSinkWorker(@NonNull String name,
                             @NonNull StreamConsumer consumer,
                             @NonNull SinkHandler handler,
                             @NonNull CircuitBreaker circuitBreaker) {
        this.name = name;
        this.consumer = consumer;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
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
        var batch = this.consumer.poll(BATCH_SIZE, Duration.ofMillis(50));
        long lo = this.lastOffset.get();
        if (lo >= 0) {
            batch = batch.stream().filter(r -> r.getOffset() > lo).toList();
        }
        if (!batch.isEmpty()) {
            processBatch(batch);
            long offset = this.lastOffset.get();
            if (offset >= 0) {
                this.consumer.commit(offset);
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void processBatch(List<ConsumerRecord> batch) {
        for (var record : batch) {
            if (!processRecord(record)) {
                return;
            }
        }
    }

    private boolean processRecord(ConsumerRecord record) {
        while (this.eventLoop.getState() == EventLoopState.RUNNING) {
            if (this.circuitBreaker.isOpen()) {
                LockSupport.parkNanos(200_000_000L);
                continue;
            }
            SinkResult result = this.handler.handle(record).join();
            switch (result.getStatus()) {
                case SUCCESS:
                    this.lastOffset.set(record.getOffset());
                    return true;
                case BACKOFF:
                    break;
                case INTERRUPTED:
                case FAILURE:
                    return false;
            }
        }
        return false;
    }

}
