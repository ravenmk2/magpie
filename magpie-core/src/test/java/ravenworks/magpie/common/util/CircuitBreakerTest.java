package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {

    @Test
    void initiallyClosed() {
        var cb = new CircuitBreaker("t", 3, 2, 1_000);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    void staysClosedBelowFailureThreshold() {
        var cb = new CircuitBreaker("t", 3, 2, 1_000);
        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    void opensAtFailureThreshold() {
        var cb = new CircuitBreaker("t", 3, 2, 1_000);
        cb.recordFailure();
        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());
    }

    @Test
    void successResetsConsecutiveFailures() {
        var cb = new CircuitBreaker("t", 3, 2, 1_000);
        cb.recordFailure();
        cb.recordFailure();
        cb.recordSuccess();
        cb.recordFailure();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    void transitionsToHalfOpenAfterResetElapsed() throws Exception {
        var cb = new CircuitBreaker("t", 1, 2, 50);
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());

        Thread.sleep(80);
        assertFalse(cb.isOpen());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
    }

    @Test
    void halfOpenClosesAfterEnoughSuccesses() throws Exception {
        var cb = new CircuitBreaker("t", 1, 2, 50);
        cb.recordFailure();
        Thread.sleep(80);
        assertFalse(cb.isOpen());

        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    void halfOpenFailureReopens() throws Exception {
        var cb = new CircuitBreaker("t", 1, 2, 50);
        cb.recordFailure();
        Thread.sleep(80);
        assertFalse(cb.isOpen());

        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());
    }

    @Test
    void concurrentFailuresReachThresholdWithoutLostUpdates() throws Exception {
        int threads = 64;
        var cb = new CircuitBreaker("t", threads, 1, 60_000);
        var start = new CountDownLatch(1);
        // close() 等待全部任务完成
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    await(start);
                    cb.recordFailure();
                });
            }
            start.countDown();
        }

        // 计数丢失会停在 CLOSED；恰好 threshold 次并发失败必须打开
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());
    }

    @Test
    void concurrentHalfOpenSuccessesCloseWithoutLostUpdates() throws Exception {
        int threads = 64;
        var cb = new CircuitBreaker("t", 1, threads, 10);
        cb.recordFailure();
        Thread.sleep(30);
        assertFalse(cb.isOpen()); // → HALF_OPEN
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    await(start);
                    cb.recordSuccess();
                });
            }
            start.countDown();
        }

        // 计数丢失会停在 HALF_OPEN；恰好 halfOpenSuccessCount 次并发成功必须闭合
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
