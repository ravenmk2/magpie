package ravenworks.magpie.engine.source.mysql;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.SendResult;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceConnector implements SourceConnector {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {

    };

    private static final String DEFAULT_TABLE_NAME = "magpie_outbox_message";
    private static final int DEFAULT_POLL_INTERVAL_MS = 5_000;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_RETRY_DELAY_MS = 300_000;
    private static final Object POLL_SIGNAL = new Object();
    private static final String STRATEGY_KEY_ORDERED = "key_ordered";
    private static final String STRATEGY_BEST_EFFORT = "best_effort";


    private enum SendStrategy {KEY_ORDERED, BEST_EFFORT}


    private final String name;
    private final StreamProducer producer;
    private final String tableName;
    private final String url;
    private final String username;
    private final String password;
    private final int batchSize;
    private final int retryDelay;
    private final SendStrategy sendStrategy;
    private final EventLoop eventLoop;

    private Connection connection;
    private long availableAt;

    public MySqlPollSourceConnector(@NonNull StreamProducer producer,
                                    @NonNull String name,
                                    @NonNull Map<String, Object> properties) {
        this.name = name;
        this.producer = producer;
        this.tableName = getStringProperty(properties, "tableName", DEFAULT_TABLE_NAME);
        this.url = getStringProperty(properties, "url", null);
        if (this.url == null || this.url.isEmpty()) {
            throw new IllegalArgumentException("Property 'url' is required for MySQL poll source");
        }
        this.username = getStringProperty(properties, "username", "");
        this.password = getStringProperty(properties, "password", "");
        this.batchSize = getIntProperty(properties, "batchSize", DEFAULT_BATCH_SIZE);
        this.retryDelay = getIntProperty(properties, "retryDelay", DEFAULT_RETRY_DELAY_MS);
        this.sendStrategy = parseSendStrategy(getStringProperty(properties, "sendStrategy", STRATEGY_BEST_EFFORT));
        int pollInterval = getIntProperty(properties, "pollInterval", DEFAULT_POLL_INTERVAL_MS);
        this.eventLoop = new EventLoop("src-" + name, pollInterval, this::dispatch);
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
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof EventLoop.Idle) {
            this.eventLoop.enqueue(POLL_SIGNAL);
        } else if (event == POLL_SIGNAL) {
            try {
                this.doPoll();
            } catch (Exception e) {
                log.error("Poll failed for source '{}'", this.name, e);
                this.availableAt = System.currentTimeMillis() + this.retryDelay;
            }
        } else if (event instanceof EventLoop.PreShutdown) {
            this.closeConnection();
        }
    }

    private void doPoll() {
        if (System.currentTimeMillis() < this.availableAt) {
            return;
        }
        if (!ensureConnection()) {
            return;
        }
        var records = queryBatch();
        if (records.isEmpty()) {
            return;
        }
        var futures = this.sendStrategy == SendStrategy.KEY_ORDERED
                ? sendBatchKeyOrdered(records)
                : sendBatchBestEffort(records);

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

        if (checkAllSucceeded(futures)) {
            deleteBatch(records);
            if (records.size() == this.batchSize) {
                this.eventLoop.enqueue(POLL_SIGNAL);
            }
        } else {
            this.availableAt = System.currentTimeMillis() + this.retryDelay;
            log.error("Send batch failed for source '{}', retry after {}", this.name, this.availableAt);
        }
    }

    private List<OutboxRecord> queryBatch() {
        List<OutboxRecord> records = new ArrayList<>();
        String sql = "SELECT id, type, event_time, topic, tenant_id, business_key, headers, payload FROM " + this.tableName + " ORDER BY id ASC, event_time ASC LIMIT ?";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            ps.setInt(1, this.batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var record = new OutboxRecord();
                    record.id = rs.getString("id");
                    record.type = rs.getString("type");
                    Timestamp ts = rs.getTimestamp("event_time");
                    record.eventTime = ts != null ? ts.toLocalDateTime() : null;
                    record.topic = rs.getString("topic");
                    record.tenantId = rs.getString("tenant_id");
                    record.businessKey = rs.getString("business_key");
                    String headersJson = rs.getString("headers");
                    record.headers = JsonUtils.decode(headersJson, HEADERS_TYPE);
                    record.payload = rs.getString("payload");
                    records.add(record);
                }
            }
        } catch (SQLException e) {
            log.error("Query batch failed for source '{}'", this.name, e);
            this.connection = null;
        }
        return records;
    }

    private List<CompletableFuture<SendResult>> sendBatchBestEffort(List<OutboxRecord> records) {
        List<CompletableFuture<SendResult>> futures = new ArrayList<>();
        for (var r : records) {
            futures.add(this.producer.send(buildMessage(r)));
        }
        return futures;
    }

    private List<CompletableFuture<SendResult>> sendBatchKeyOrdered(List<OutboxRecord> records) {
        List<CompletableFuture<SendResult>> allFutures = new ArrayList<>();
        List<CompletableFuture<SendResult>> currentBatch = new ArrayList<>();
        Set<String> seenKeys = new HashSet<>();

        for (var r : records) {
            String key = r.businessKey != null && !r.businessKey.isEmpty()
                    ? r.businessKey
                    : r.id;
            if (seenKeys.contains(key)) {
                flushSubBatch(currentBatch, allFutures);
                currentBatch.clear();
                seenKeys.clear();
                if (!checkAllSucceeded(allFutures)) {
                    return allFutures;
                }
            }
            seenKeys.add(key);
            currentBatch.add(this.producer.send(buildMessage(r)));
        }
        flushSubBatch(currentBatch, allFutures);
        return allFutures;
    }

    private void flushSubBatch(List<CompletableFuture<SendResult>> batch,
                               List<CompletableFuture<SendResult>> allFutures) {
        if (batch.isEmpty()) {
            return;
        }
        CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new)).join();
        allFutures.addAll(batch);
    }

    private boolean checkAllSucceeded(List<CompletableFuture<SendResult>> futures) {
        for (var future : futures) {
            SendResult result = future.getNow(null);
            if (result == null || !result.isSucceeded()) {
                return false;
            }
        }
        return true;
    }

    private MessageRecord buildMessage(OutboxRecord r) {
        return new MessageRecord()
                .setId(r.id)
                .setType(r.type)
                .setEventTime(r.eventTime)
                .setTopic(r.topic)
                .setTenantId(r.tenantId)
                .setBusinessKey(r.businessKey)
                .setHeaders(r.headers)
                .setPayload(r.payload != null ? r.payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    private void deleteBatch(List<OutboxRecord> records) {
        List<String> ids = records.stream()
                .map(r -> r.id)
                .toList();
        String placeholders = records.stream()
                .map(r -> "?")
                .collect(Collectors.joining(","));
        String sql = "DELETE FROM " + this.tableName + " WHERE id IN (" + placeholders + ")";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Delete batch failed for source '{}'", this.name, e);
            this.connection = null;
        }
    }

    private boolean ensureConnection() {
        if (this.connection != null) {
            try {
                if (!this.connection.isClosed() && this.connection.isValid(2)) {
                    return true;
                }
            } catch (SQLException ignored) {
            }
            closeConnection();
        }
        try {
            this.connection = DriverManager.getConnection(this.url, this.username, this.password);
            log.info("Connected to MySQL for source '{}'", this.name);
            return true;
        } catch (SQLException e) {
            log.error("Failed to connect to MySQL for source '{}': {}", this.name, e.getMessage());
            return false;
        }
    }

    private void closeConnection() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException ignored) {
            }
            this.connection = null;
        }
    }

    private static int getIntProperty(Map<String, Object> properties, String key, int defaultValue) {
        Object value = properties.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }

    private static String getStringProperty(Map<String, Object> properties, String key, String defaultValue) {
        Object value = properties.get(key);
        if (value instanceof String s) {
            return s;
        }
        return defaultValue;
    }

    private static SendStrategy parseSendStrategy(String value) {
        if (STRATEGY_KEY_ORDERED.equalsIgnoreCase(value)) {
            return SendStrategy.KEY_ORDERED;
        }
        return SendStrategy.BEST_EFFORT;
    }


    private static class OutboxRecord {

        String id;
        String type;
        LocalDateTime eventTime;
        String topic;
        String tenantId;
        String businessKey;
        Map<String, String> headers;
        String payload;

    }

}
