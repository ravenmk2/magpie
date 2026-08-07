package ravenworks.magpie.engine.impl.source.mysql;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceConnector implements SourceConnector {

    private static final Object POLL_SIGNAL = new Object();
    private static final String STRATEGY_ORDERED = "ordered";
    private static final String STRATEGY_KEY_ORDERED = "key_ordered";

    private final String name;
    private final StreamProducer producer;
    private final int batchSize;
    private final int retryDelay;
    private final SendStrategy sendStrategy;
    private final OutboxStore outboxStore;
    private final WorkLoop workLoop;

    private long availableAt;

    public MySqlPollSourceConnector(@NonNull StreamProducer producer,
                                    @NonNull String name,
                                    @NonNull Map<String, Object> properties) {
        this.name = name;
        this.producer = producer;
        var p = new MySqlPollProperties();
        PropertiesUtils.bind(p, properties);
        if (p.getUrl() == null || p.getUrl().isEmpty()) {
            throw new IllegalArgumentException("Property 'url' is required for MySQL poll source");
        }
        this.outboxStore = new OutboxStore(name, p.getTableName(), p.getUrl(), p.getUsername(), p.getPassword());
        this.batchSize = p.getBatchSize();
        this.retryDelay = p.getRetryDelay();
        this.sendStrategy = parseSendStrategy(p.getSendStrategy());
        this.workLoop = new WorkLoop("src-" + name, p.getPollInterval(), this::dispatch);
        log.info("Source '{}' initialized, sendStrategy={}", this.name, p.getSendStrategy());
    }

    @Override
    public String type() {
        return "mysql-poll";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.workLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof WorkLoop.Started) {
            this.outboxStore.ensureConnection();
        } else if (event instanceof WorkLoop.Idle) {
            if (this.workLoop.getState() == WorkLoopState.RUNNING) {
                this.workLoop.enqueue(POLL_SIGNAL);
            }
        } else if (event == POLL_SIGNAL) {
            try {
                this.doPoll();
            } catch (Exception e) {
                log.error("Poll failed for source '{}'", this.name, e);
                this.availableAt = System.currentTimeMillis() + this.retryDelay;
            }
        } else if (event instanceof WorkLoop.PreShutdown) {
            this.outboxStore.close();
        }
    }

    private void doPoll() {
        if (this.workLoop.getState() != WorkLoopState.RUNNING) {
            return;
        }
        if (System.currentTimeMillis() < this.availableAt) {
            return;
        }
        if (!this.outboxStore.ensureConnection()) {
            return;
        }
        var records = this.outboxStore.queryBatch(this.batchSize);
        if (records.isEmpty()) {
            return;
        }
        var subBatches = this.sendStrategy.partition(records);

        for (var subBatch : subBatches) {
            var futures = sendSubBatch(subBatch);
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            var succeededIds = collectSucceededIds(subBatch, futures);
            if (!succeededIds.isEmpty()) {
                this.outboxStore.deleteBatch(succeededIds);
            }
            if (succeededIds.size() < subBatch.size()) {
                this.availableAt = System.currentTimeMillis() + this.retryDelay;
                log.error("Send batch failed for source '{}', retry after {}", this.name, this.availableAt);
                return;
            }
            if (this.workLoop.getState() != WorkLoopState.RUNNING) {
                return;
            }
        }

        if (records.size() == this.batchSize) {
            this.workLoop.enqueue(POLL_SIGNAL);
        }
    }

    private List<CompletableFuture<SendResult>> sendSubBatch(List<OutboxRecord> subBatch) {
        var futures = new ArrayList<CompletableFuture<SendResult>>(subBatch.size());
        for (var record : subBatch) {
            futures.add(this.producer.send(buildMessage(record)));
        }
        return futures;
    }

    private static List<String> collectSucceededIds(List<OutboxRecord> subBatch,
                                                    List<CompletableFuture<SendResult>> futures) {
        var ids = new ArrayList<String>(futures.size());
        for (int i = 0; i < futures.size(); i++) {
            SendResult result = futures.get(i).getNow(null);
            if (result != null && result.isSucceeded()) {
                ids.add(subBatch.get(i).getId());
            }
        }
        return ids;
    }

    private static MessageRecord buildMessage(OutboxRecord r) {
        return new MessageRecord()
                .setId(r.getId())
                .setType(r.getType())
                .setEventTime(r.getEventTime())
                .setTopic(r.getTopic())
                .setTenantId(r.getTenantId())
                .setBusinessKey(r.getBusinessKey())
                .setHeaders(r.getHeaders())
                .setPayload(r.getPayload() != null ? r.getPayload().getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    private static SendStrategy parseSendStrategy(String value) {
        if (STRATEGY_ORDERED.equalsIgnoreCase(value)) {
            return new SendStrategy.OrderedStrategy();
        }
        if (STRATEGY_KEY_ORDERED.equalsIgnoreCase(value)) {
            return new SendStrategy.KeyOrderedStrategy();
        }
        return new SendStrategy.BestEffortStrategy();
    }

}
