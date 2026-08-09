package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


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
                    awaitLatch(start);
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
                    awaitLatch(start);
                    cb.recordSuccess();
                });
            }
            start.countDown();
        }

        // 计数丢失会停在 HALF_OPEN；恰好 halfOpenSuccessCount 次并发成功必须闭合
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    void recordFailureWhileOpenDoesNotExtendOpenWindow() throws Exception {
        // 固定窗口语义：OPEN 状态下的 recordFailure 只累计计数，不刷新 openUntilTimestamp
        var cb = new CircuitBreaker("t", 1, 1, 300);
        long openedAt = System.currentTimeMillis();
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        Thread.sleep(150);
        cb.recordFailure(); // OPEN 中再次失败；窗口若被延长，恢复点会推迟到 t+450ms
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        // 窗口未延长：仍在首次失败后 300ms 处转入 HALF_OPEN。
        // 用绝对截止时间判定：非延长 → openedAt+300 前恢复；延长 → 要等到 openedAt+450
        boolean halfOpen = false;
        while (System.currentTimeMillis() < openedAt + 400) {
            if (!cb.isOpen()) {
                halfOpen = true;
                break;
            }
            Thread.sleep(5);
        }
        assertTrue(halfOpen, "open window was extended by recordFailure while OPEN");
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
    }

    @Test
    void recordSuccessWhileOpenStaysOpen() {
        // OPEN 状态下的 recordSuccess 不改变状态；其累计的成功数也不会带进半开：
        // 窗口结束时 transitionToHalfOpen 会将 consecutiveSuccesses 清零
        var cb = new CircuitBreaker("t", 1, 1, 100);
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());

        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());

        // 窗口结束 → HALF_OPEN；halfOpenSuccessCount=1，仍需一次新的成功才能闭合
        await().atMost(2, TimeUnit.SECONDS).until(() -> !cb.isOpen());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    void degenerateConfigThresholdOneSuccessOne() {
        // 最敏感配置：一次失败即熔断，窗口结束后一次成功即闭合
        var cb = new CircuitBreaker("t", 1, 1, 50);
        cb.recordFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());

        await().atMost(2, TimeUnit.SECONDS).until(() -> !cb.isOpen());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());

        cb.recordSuccess();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
