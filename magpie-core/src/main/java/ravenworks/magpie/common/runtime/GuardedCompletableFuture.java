package ravenworks.magpie.common.runtime;

import lombok.NonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;


/**
 * A {@link CompletableFuture} that rejects blocking waits ({@code get} / {@code join})
 * from a designated thread, turning potential self-deadlocks into fast failures.
 *
 * <p>Typical use: an event loop's termination future must never be awaited from the
 * loop's own thread. The guarded thread is supplied lazily so it can be resolved
 * after construction (e.g. assigned when the loop starts); a {@code null} thread
 * disables the check.
 *
 * <p>Note: only direct blocking waits on this future are guarded. Composing it into
 * another future (e.g. {@code CompletableFuture.allOf(...).join()}) bypasses the check.
 */
public class GuardedCompletableFuture<T> extends CompletableFuture<T> {

    private final Supplier<Thread> guardedThread;
    private final String name;

    public GuardedCompletableFuture(@NonNull Supplier<Thread> guardedThread,
                                    @NonNull String name) {
        this.guardedThread = guardedThread;
        this.name = name;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        checkThread();
        return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        checkThread();
        return super.get(timeout, unit);
    }

    @Override
    public T join() {
        checkThread();
        return super.join();
    }

    private void checkThread() {
        if (Thread.currentThread() == this.guardedThread.get()) {
            throw new IllegalStateException(
                    this.name + " - Cannot block on this future from the guarded thread");
        }
    }

}
