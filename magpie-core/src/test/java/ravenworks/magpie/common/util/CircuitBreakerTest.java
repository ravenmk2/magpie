package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

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

}
