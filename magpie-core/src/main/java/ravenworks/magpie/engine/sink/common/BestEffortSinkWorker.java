package ravenworks.magpie.engine.sink.common;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.sink.SinkHandler;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class BestEffortSinkWorker implements SinkWorker {

    private static final Object POLL_SIGNAL = new Object();

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final RetryMessageStore retryStore;
    private final EventLoop eventLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);

    private volatile Thread loopThread;

    public BestEffortSinkWorker(@NonNull String name,
                                @NonNull StreamConsumer consumer,
                                @NonNull SinkHandler handler,
                                @NonNull CircuitBreaker circuitBreaker,
                                @NonNull RetryMessageStore retryStore) {
        this.name = name;
        this.consumer = consumer;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
        this.retryStore = retryStore;
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
            this.poll();
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
        this.poll();
    }

    private void onPreShutdown() {
        long lastOffset = this.lastOffset.get();
        if (lastOffset >= 0) {
            this.consumer.commit(lastOffset + 1);
        }
        try {
            this.consumer.stop();
        } catch (Exception ex) {
            log.warn("[{}] error stopping consumer", this.name, ex);
        }
    }

    private void poll() {

    }

}
