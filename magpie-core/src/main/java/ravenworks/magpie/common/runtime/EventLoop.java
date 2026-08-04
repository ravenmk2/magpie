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
public class EventLoop {

    private static final Object NOOP = new Object();

    private final AtomicReference<EventLoopState> state = new AtomicReference<>(EventLoopState.NEW);
    private final BlockingQueue<Object> events = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> termination;

    @Getter
    private final String name;
    private final int idleTimeout;
    private final Consumer<Object> handler;

    private volatile Thread thread;

    public EventLoop(@NonNull String name,
                     int idleTimeout,
                     @NonNull Consumer<Object> handler) {
        this.name = name;
        this.idleTimeout = Math.max(10, idleTimeout);
        this.handler = handler;
        this.termination = new GuardedCompletableFuture<>(() -> this.thread, this.name);
    }

    public EventLoopState getState() {
        return this.state.get();
    }

    public void start() {
        if (this.state.compareAndSet(EventLoopState.NEW, EventLoopState.RUNNING)) {
            this.thread = Thread.ofVirtual()
                    .name(this.name)
                    .start(this::run);
        }
    }

    public CompletableFuture<Void> shutdown() {
        if (this.state.compareAndSet(EventLoopState.NEW, EventLoopState.TERMINATED)) {
            this.termination.complete(null);
            return this.termination;
        }
        var prev = this.state.getAndUpdate(s ->
                s == EventLoopState.RUNNING ? EventLoopState.SHUTTING_DOWN : s);
        if (prev == EventLoopState.TERMINATED || prev == EventLoopState.SHUTTING_DOWN) {
            return this.termination;
        }
        if (prev != EventLoopState.RUNNING) {
            throw new IllegalStateException("Event loop is not running");
        }
        log.info("{} - Event loop shutdown requested", this.name);
        this.events.add(NOOP);
        return this.termination;
    }

    public void enqueue(@NonNull Object event) {
        synchronized (this.events) {
            var s = this.state.get();
            if (s == EventLoopState.SHUTTING_DOWN || s == EventLoopState.TERMINATED) {
                log.warn("{} - Event dropped, event loop is shutting down: {}",
                        this.name, event.getClass().getSimpleName());
                return;
            }
            this.events.add(event);
        }
    }

    private void run() {
        log.info("{} - Event loop started", this.name);
        this.dispatch(Started.INSTANCE);
        while (this.state.get() == EventLoopState.RUNNING) {
            Object msg = this.poll(this.idleTimeout);
            this.dispatch(msg);
        }

        log.info("{} - Event loop shutdown initiated", this.name);
        this.dispatch(PreShutdown.INSTANCE);
        while (true) {
            Object msg;
            synchronized (this.events) {
                msg = this.events.poll();
                if (msg == null) {
                    break;
                }
            }
            if (msg instanceof Idle) {
                continue;
            }
            this.dispatch(msg);
        }

        this.dispatch(Terminated.INSTANCE);
        this.state.set(EventLoopState.TERMINATED);
        log.info("{} - Event loop exited", this.name);
        this.termination.complete(null);
    }

    private Object poll(int timeout) {
        try {
            Object msg = this.events.poll(timeout, TimeUnit.MILLISECONDS);
            return msg == null ? Idle.INSTANCE : msg;
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    private void dispatch(Object msg) {
        if (msg == null || msg == NOOP) {
            return;
        }
        try {
            this.handler.accept(msg);
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }


    public record Idle() {

        private static final Idle INSTANCE = new Idle();

    }


    public record Started() {

        private static final Started INSTANCE = new Started();

    }


    public record PreShutdown() {

        private static final PreShutdown INSTANCE = new PreShutdown();

    }


    public record Terminated() {

        private static final Terminated INSTANCE = new Terminated();

    }

}
