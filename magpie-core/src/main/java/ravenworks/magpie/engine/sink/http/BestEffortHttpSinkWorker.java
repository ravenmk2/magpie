package ravenworks.magpie.engine.sink.http;

import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.sink.http.HttpSinkConnector.HttpConfig;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.time.LocalDateTime;


@Slf4j
class BestEffortHttpSinkWorker extends AbstractHttpSinkWorker {

    private enum State {NORMAL, RETRYING}


    private final RetryMessageStore retryStore;
    private final int inplaceAttempts;
    private final int emptyPollThreshold;

    private State state;
    private int emptyPollCount;

    BestEffortHttpSinkWorker(String name, int partition, StreamConsumer consumer,
                             RetryMessageStore retryStore, HttpConfig config) {
        super(name, partition, consumer, config);
        this.retryStore = retryStore;
        this.inplaceAttempts = config.retryInplaceAttempts;
        this.emptyPollThreshold = config.emptyPollThreshold;
    }

    @Override
    public void start() {
        super.start();
        this.state = State.RETRYING;
    }

    @Override
    void pollAndProcess() {
        if (this.circuitBreaker.isOpen()) {
            return;
        }
        if (this.state == State.RETRYING) {
            pollAndProcessRetrying();
        } else {
            pollAndProcessNormal();
        }
    }

    @Override
    protected void onPreShutdown() {
        try {
            this.consumer.stop();
        } catch (Exception e) {
            log.warn("[{}] partition={} error stopping consumer", this.name, this.partition, e);
        }
    }

    private void pollAndProcessNormal() {
        var batch = this.consumer.poll(BATCH_SIZE, Duration.ofMillis(50));
        log.error("Polled {} messages", batch.size());
        long lastCompletedOffset = processBatch(batch);
        if (!batch.isEmpty()) {
            if (lastCompletedOffset >= 0) {
                this.consumer.commit(lastCompletedOffset);
            }
            this.emptyPollCount = 0;
        } else {
            this.emptyPollCount++;
            if (this.emptyPollCount >= this.emptyPollThreshold) {
                enterRetrying();
                return;
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private long processBatch(java.util.List<ConsumerRecord> batch) {
        long lastCompletedOffset = -1;
        for (var record : batch) {
            if (inplaceRetry(record, this.inplaceAttempts)) {
                lastCompletedOffset = record.getOffset();
            } else {
                this.retryStore.save(this.name, record);
            }
        }
        return lastCompletedOffset;
    }

    private void enterRetrying() {
        this.state = State.RETRYING;
        log.info("[{}] partition={} entering RETRYING mode", this.name, this.partition);
    }

    private void pollAndProcessRetrying() {
        var entries = this.retryStore.listRetryable(this.name, BATCH_SIZE);
        if (entries.isEmpty()) {
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            log.info("[{}] partition={} exiting RETRYING mode", this.name, this.partition);
            return;
        }
        for (var entry : entries) {
            var record = new ConsumerRecord()
                    .setId(entry.getMessageId())
                    .setType(entry.getType())
                    .setEventTime(entry.getEventTime())
                    .setTopic(entry.getTopic())
                    .setTenantId(entry.getTenantId())
                    .setBusinessKey(entry.getBusinessKey())
                    .setHeaders(entry.getHeaders())
                    .setPayload(entry.getPayload());
            DeliverResult result = deliver(record, entry.getAttempts() + 1);
            if (result == DeliverResult.SUCCESS) {
                this.retryStore.succeeded(entry.getId());
            } else {
                this.retryStore.failed(entry.getId(), computeRetryAt(entry.getAttempts() + 1));
                log.warn("[{}] partition={} retry failed for {}", this.name, this.partition, entry.getId());
                return;
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private LocalDateTime computeRetryAt(int attempts) {
        long delayMs = computeBackoffDelay(this.config.backoff, this.config.delay, this.config.maxDelay, attempts);
        return LocalDateTime.now().plusNanos(delayMs * 1_000_000);
    }

}
