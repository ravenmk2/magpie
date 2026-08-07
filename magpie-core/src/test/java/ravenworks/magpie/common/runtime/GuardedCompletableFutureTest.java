package ravenworks.magpie.common.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class GuardedCompletableFutureTest {

    @Test
    void blockingFromGuardedThreadThrows() {
        Thread guarded = Thread.currentThread();
        var future = new GuardedCompletableFuture<String>(() -> guarded, "t-guarded");
        assertThrows(IllegalStateException.class, future::join);
        assertThrows(IllegalStateException.class, future::get);
        assertThrows(IllegalStateException.class, () -> future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void blockingFromOtherThreadAllowed() throws Exception {
        Thread guarded = Thread.currentThread();
        var future = new GuardedCompletableFuture<String>(() -> guarded, "t-other");
        future.complete("ok");
        var result = new java.util.concurrent.atomic.AtomicReference<String>();
        var t = Thread.ofVirtual().start(() -> result.set(future.join()));
        t.join(2_000);
        assertEquals("ok", result.get());
    }

    @Test
    void nullGuardedThreadDisablesCheck() throws Exception {
        var future = new GuardedCompletableFuture<String>(() -> null, "t-null");
        future.complete("ok");
        assertEquals("ok", future.get());
        assertEquals("ok", future.join());
        assertEquals("ok", future.get(1, TimeUnit.SECONDS));
    }

}
