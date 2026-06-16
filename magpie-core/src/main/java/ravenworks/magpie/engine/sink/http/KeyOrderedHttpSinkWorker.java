package ravenworks.magpie.engine.sink.http;

import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.sink.http.HttpSinkConnector.HttpConfig;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;


@Slf4j
class KeyOrderedHttpSinkWorker extends AbstractHttpSinkWorker {

    private enum State {NORMAL, RETRYING}


    private final RetryMessageStore retryStore;
    private final Set<String> blockedKeys = new HashSet<>();
    private final int inplaceAttempts;
    private final int emptyPollThreshold;

    private State state;
    private int emptyPollCount;

    KeyOrderedHttpSinkWorker(String name, int partition, StreamConsumer consumer,
                             RetryMessageStore retryStore, HttpConfig config) {
        super(name, partition, consumer, config);
        this.retryStore = retryStore;
        this.inplaceAttempts = config.retryInplaceAttempts;
        this.emptyPollThreshold = config.emptyPollThreshold;
    }

    @Override
    public void start() {
        super.start();
        this.blockedKeys.addAll(this.retryStore.listKeys(this.name));
        if (!this.blockedKeys.isEmpty()) {
            this.state = State.RETRYING;
            log.info("[{}] partition={} entering RETRYING mode, {} blocked keys",
                    this.name, this.partition, this.blockedKeys.size());
        } else {
            this.state = State.NORMAL;
        }
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
            String key = record.getBusinessKey();
            if (this.blockedKeys.contains(key)) {
                this.retryStore.save(this.name, record);
                continue;
            }
            if (inplaceRetry(record, this.inplaceAttempts)) {
                lastCompletedOffset = record.getOffset();
            } else {
                this.retryStore.save(this.name, record);
                this.blockedKeys.add(key);
            }
        }
        return lastCompletedOffset;
    }

    private void enterRetrying() {
        this.state = State.RETRYING;
        log.info("[{}] partition={} entering RETRYING mode", this.name, this.partition);
    }

    private void pollAndProcessRetrying() {
        var entries = this.retryStore.list(this.name, BATCH_SIZE);
        if (entries.isEmpty()) {
            this.blockedKeys.clear();
            this.blockedKeys.addAll(this.retryStore.listKeys(this.name));
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            log.info("[{}] partition={} exiting RETRYING mode, {} blocked keys",
                    this.name, this.partition, this.blockedKeys.size());
            return;
        }
        for (var entry : entries) {
            DeliverResult result = deliver(entry);
            if (result == DeliverResult.SUCCESS) {
                this.retryStore.succeeded(entry.getId());
            } else {
                log.warn("[{}] partition={} retry failed for {}", this.name, this.partition, entry.getId());
                return;
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private DeliverResult deliver(ravenworks.magpie.engine.retry.RetryRecord retryRecord) {
        var record = new ConsumerRecord()
                .setOffset(retryRecord.getOffset())
                .setId(retryRecord.getMessageId())
                .setType(retryRecord.getType())
                .setEventTime(retryRecord.getEventTime())
                .setTopic(retryRecord.getTopic())
                .setTenantId(retryRecord.getTenantId())
                .setBusinessKey(retryRecord.getBusinessKey())
                .setHeaders(retryRecord.getHeaders())
                .setPayload(retryRecord.getPayload());
        return deliver(record, retryRecord.getAttempts() + 1);
    }

}
