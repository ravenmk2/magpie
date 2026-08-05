package ravenworks.magpie.common.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;


/**
 * 熔断器。批量投递模式下会被多个 handler 线程与 worker 线程并发访问，
 * 所有读写（含状态迁移）都在 monitor 内完成；临界区仅内存操作，无 IO。
 *
 * @author Raven
 */
@Slf4j
public class CircuitBreaker {

    public enum State {CLOSED, OPEN, HALF_OPEN}


    private final String name;
    private final int failureThreshold;
    private final int halfOpenSuccessCount;
    private final long resetMillis;

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private int consecutiveSuccesses;
    private long openUntilTimestamp;

    public CircuitBreaker(@NonNull String name, int failureThreshold, int halfOpenSuccessCount, long resetMillis) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.halfOpenSuccessCount = halfOpenSuccessCount;
        this.resetMillis = resetMillis;
    }

    public synchronized State getState() {
        return this.state;
    }

    public synchronized boolean isOpen() {
        if (this.state == State.OPEN) {
            if (System.currentTimeMillis() >= this.openUntilTimestamp) {
                transitionToHalfOpen();
                return false;
            }
            return true;
        }
        return false;
    }

    public synchronized void recordSuccess() {
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses++;
        if (this.state == State.HALF_OPEN && this.consecutiveSuccesses >= this.halfOpenSuccessCount) {
            transitionToClosed();
        }
    }

    public synchronized void recordFailure() {
        this.consecutiveSuccesses = 0;
        this.consecutiveFailures++;
        if (this.state == State.HALF_OPEN) {
            transitionToOpen();
        } else if (this.state == State.CLOSED && this.consecutiveFailures >= this.failureThreshold) {
            transitionToOpen();
        }
    }

    private void transitionToOpen() {
        this.state = State.OPEN;
        this.openUntilTimestamp = System.currentTimeMillis() + this.resetMillis;
        log.warn("[{}] circuit breaker OPEN, resume in {}ms", this.name, this.resetMillis);
    }

    private void transitionToHalfOpen() {
        this.state = State.HALF_OPEN;
        this.consecutiveSuccesses = 0;
        log.info("[{}] circuit breaker HALF_OPEN, probing", this.name);
    }

    private void transitionToClosed() {
        this.state = State.CLOSED;
        this.consecutiveFailures = 0;
        log.info("[{}] circuit breaker CLOSED", this.name);
    }

}
