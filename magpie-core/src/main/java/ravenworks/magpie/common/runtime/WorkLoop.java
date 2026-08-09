package ravenworks.magpie.common.runtime;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


/**
 * @author Raven
 */
@Slf4j
public class WorkLoop implements Lifecycle {

    private static final Object NOOP = new Object();

    private final AtomicReference<WorkLoopState> state = new AtomicReference<>(WorkLoopState.NEW);
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> termination;

    @Getter
    private final String name;
    private final int idleTimeout;
    private final Consumer<Object> handler;

    private volatile Thread thread;

    public WorkLoop(@NonNull String name,
                    int idleTimeout,
                    @NonNull Consumer<Object> handler) {
        this.name = name;
        this.idleTimeout = Math.max(10, idleTimeout);
        this.handler = handler;
        this.termination = new GuardedCompletableFuture<>(() -> this.thread, this.name);
    }

    public WorkLoopState getState() {
        return this.state.get();
    }

    @Override
    public void start() {
        if (this.state.compareAndSet(WorkLoopState.NEW, WorkLoopState.RUNNING)) {
            this.thread = Thread.ofVirtual()
                    .name(this.name)
                    .start(this::run);
            return;
        }
        throw new IllegalStateException("Work loop is not in NEW state: " + this.state.get());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        if (this.state.compareAndSet(WorkLoopState.NEW, WorkLoopState.TERMINATED)) {
            this.termination.complete(null);
            return this.termination;
        }
        var prev = this.state.getAndUpdate(s ->
                s == WorkLoopState.RUNNING ? WorkLoopState.SHUTTING_DOWN : s);
        if (prev == WorkLoopState.TERMINATED || prev == WorkLoopState.SHUTTING_DOWN) {
            return this.termination;
        }
        if (prev != WorkLoopState.RUNNING) {
            throw new IllegalStateException("Work loop is not running");
        }
        log.info("{} - Work loop shutdown requested", this.name);
        this.queue.add(NOOP);
        return this.termination;
    }

    public void enqueue(@NonNull Object message) {
        synchronized (this.queue) {
            var s = this.state.get();
            if (s == WorkLoopState.SHUTTING_DOWN || s == WorkLoopState.TERMINATED) {
                log.warn("{} - Message dropped, work loop is shutting down: {}",
                        this.name, message.getClass().getSimpleName());
                return;
            }
            this.queue.add(message);
        }
    }

    private void run() {
        try {
            this.doRun();
        } catch (Throwable e) {
            log.error("{} - Work loop died", this.name, e);
            this.termination.completeExceptionally(e);
        } finally {
            this.state.set(WorkLoopState.TERMINATED);
            log.info("{} - Work loop exited", this.name);
            this.termination.complete(null);
        }
    }

    private void doRun() {
        log.info("{} - Work loop started", this.name);
        this.dispatch(WorkLoopSignal.STARTED);
        while (this.state.get() == WorkLoopState.RUNNING) {
            Object msg = this.poll(this.idleTimeout);
            this.dispatch(msg);
        }

        log.info("{} - Work loop shutdown initiated", this.name);
        this.dispatch(WorkLoopSignal.PRE_SHUTDOWN);
        this.drain();
        this.dispatch(WorkLoopSignal.TERMINATED);
    }

    private void drain() {
        while (true) {
            Object msg;
            synchronized (this.queue) {
                msg = this.queue.poll();
                if (msg == null) {
                    break;
                }
            }
            if (msg == WorkLoopSignal.IDLE) {
                continue;
            }
            this.dispatch(msg);
        }
    }

    private Object poll(int timeout) {
        try {
            Object msg = this.queue.poll(timeout, TimeUnit.MILLISECONDS);
            return msg == null ? WorkLoopSignal.IDLE : msg;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    private void dispatch(Object msg) {
        if (msg == null || msg == NOOP) {
            return;
        }
        try {
            this.handler.accept(msg);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
