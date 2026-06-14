package ravenworks.magpie.common.util;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;


/**
 * @author Raven
 */
@Slf4j
public class CircuitBreaker {

    public enum State {CLOSED, OPEN, HALF_OPEN}


    private final String name;
    private final int failureThreshold;
    private final int halfOpenSuccessCount;
    private final long resetMillis;

    @Getter
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

    public boolean isOpen() {
        if (this.state == State.OPEN) {
            if (System.currentTimeMillis() >= this.openUntilTimestamp) {
                transitionToHalfOpen();
                return false;
            }
            return true;
        }
        return false;
    }

    public void recordSuccess() {
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses++;
        if (this.state == State.HALF_OPEN && this.consecutiveSuccesses >= this.halfOpenSuccessCount) {
            transitionToClosed();
        }
    }

    public void recordFailure() {
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
