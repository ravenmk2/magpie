package ravenworks.magpie.engine.impl.source.mysql;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * 轮询 MySQL outbox 表，把消息发送到对应 Stream，成功后删除表内行。
 * 全部 DB 与发送逻辑收口在 WorkLoop 单线程，内部无需同步。
 * 最少一次语义：发送成功才删行，失败行留在表内下轮重投。
 *
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceConnector implements SourceConnector {

    private static final Object POLL_SIGNAL = new Object();

    private final String name;
    private final StreamProducer producer;
    private final MySqlPollProperties properties;
    private final WorkLoop workLoop;

    private OutboxStore store;
    private SendStrategy strategy;
    private long availableAt;
    private LocalDateTime lastDelivered;

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
        this.properties = p;
        this.workLoop = new WorkLoop("src-" + name, p.getPollInterval(), this::dispatch);
        log.info("Source '{}' initialized, sendStrategy={}, readLag={}ms", this.name, p.getSendStrategy(), p.getReadLag());
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

    @Override
    public boolean isAlive() {
        return this.workLoop.isAlive();
    }

    private void dispatch(Object event) {
        if (event == WorkLoopSignal.STARTED) {
            var p = this.properties;
            this.store = new OutboxStore(p.getUrl(), p.getUsername(), p.getPassword(), p.getTableName(), p.getReadLag());
            this.strategy = SendStrategy.of(p.getSendStrategy());
            this.workLoop.enqueue(POLL_SIGNAL);
        } else if (event == WorkLoopSignal.IDLE) {
            if (this.workLoop.getState() == WorkLoopState.RUNNING) {
                this.workLoop.enqueue(POLL_SIGNAL);
            }
        } else if (event == POLL_SIGNAL) {
            try {
                this.doPoll();
            } catch (Exception e) {
                log.error("Poll failed for source '{}'", this.name, e);
            }
        } else if (event == WorkLoopSignal.PRE_SHUTDOWN) {
            if (this.store != null) {
                this.store.close();
            }
        }
    }

    private void doPoll() {
        if (System.currentTimeMillis() < this.availableAt) {
            return;
        }
        List<OutboxRecord> batch;
        try {
            batch = this.store.queryBatch(this.properties.getBatchSize());
        } catch (Exception e) {
            // DB 故障：等待 retryDelay 再重试（OutboxStore 已失效连接，下轮自动重连）
            this.availableAt = System.currentTimeMillis() + this.properties.getRetryDelay();
            log.warn("Source '{}' failed to poll outbox, retry in {}ms", this.name, this.properties.getRetryDelay(), e);
            return;
        }
        if (batch.isEmpty()) {
            return;
        }
        this.checkLateArrival(batch);
        this.applyBusinessKeyFallback(batch);

        var sentIds = new ArrayList<String>();
        var failedKeys = new HashSet<String>();
        LocalDateTime maxSentAt = null;
        boolean failed = false;
        // 整批共享超时预算：保证停机有界（Coordinator 退役时最多等 30s）
        long deadline = System.currentTimeMillis() + this.properties.getSendTimeout();

        outer:
        for (var group : this.strategy.partition(batch)) {
            var pending = new ArrayList<Map.Entry<OutboxRecord, CompletableFuture<SendResult>>>(group.size());
            for (var record : group) {
                if (failedKeys.contains(SendStrategy.keyOf(record))) {
                    continue;
                }
                pending.add(new AbstractMap.SimpleImmutableEntry<>(record, this.send(record)));
            }
            for (var entry : pending) {
                var record = entry.getKey();
                if (this.awaitResult(entry.getValue(), deadline)) {
                    sentIds.add(record.getId());
                    if (maxSentAt == null || record.getCreatedAt().isAfter(maxSentAt)) {
                        maxSentAt = record.getCreatedAt();
                    }
                } else {
                    failed = true;
                    log.warn("Source '{}' failed to send outbox message {}, strategy={}",
                            this.name, record.getId(), this.properties.getSendStrategy());
                    switch (this.strategy.failurePolicy()) {
                        case STOP -> {
                            break outer;
                        }
                        case SKIP_KEY -> failedKeys.add(SendStrategy.keyOf(record));
                        case CONTINUE -> {
                        }
                    }
                }
            }
        }

        if (!sentIds.isEmpty()) {
            try {
                this.store.deleteBatch(sentIds);
                this.lastDelivered = maxSentAt;
            } catch (Exception e) {
                // 删除失败：已发送的行下轮重投（重复是 at-least-once 语义的一部分）
                this.availableAt = System.currentTimeMillis() + this.properties.getRetryDelay();
                log.warn("Source '{}' failed to delete sent outbox messages, they will be re-delivered", this.name, e);
                return;
            }
        }
        if (failed) {
            this.availableAt = System.currentTimeMillis() + this.properties.getRetryDelay();
        } else if (batch.size() == this.properties.getBatchSize()) {
            // 满批全部成功：立即追批，不吃 pollInterval（停机后不再追，避免丢弃告警）
            if (this.workLoop.getState() == WorkLoopState.RUNNING) {
                this.workLoop.enqueue(POLL_SIGNAL);
            }
        }
    }

    /**
     * 批次按 (created_at, id) 升序，若首行 created_at 早于已删除水位，
     * 说明有事务提交晚于 readLag 安全窗（迟到行），顺序已被打破，告警但不阻断。
     */
    private void checkLateArrival(List<OutboxRecord> batch) {
        var firstAt = batch.get(0).getCreatedAt();
        if (this.lastDelivered != null && firstAt.isBefore(this.lastDelivered)) {
            log.warn("Source '{}' read late-arriving outbox rows (created_at {} < lastDelivered {}),"
                    + " readLag may be too small for business transactions", this.name, firstAt, this.lastDelivered);
        }
    }

    /**
     * businessKeyFallbackToId 启用时，把 null/空 business_key 回退为 id:<id>。
     * 在分组与发送前统一生效，使 key_ordered 分组、分区路由、消息头的 key 保持一致。
     */
    private void applyBusinessKeyFallback(List<OutboxRecord> batch) {
        if (!this.properties.isBusinessKeyFallbackToId()) {
            return;
        }
        for (var record : batch) {
            var key = record.getBusinessKey();
            if (key == null || key.isEmpty()) {
                record.setBusinessKey("id:" + record.getId());
            }
        }
    }

    private CompletableFuture<SendResult> send(OutboxRecord record) {
        try {
            return this.producer.send(toMessage(record));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private boolean awaitResult(CompletableFuture<SendResult> future, long deadline) {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            return false;
        }
        try {
            var result = future.get(remaining, TimeUnit.MILLISECONDS);
            return result != null && result.isSucceeded();
        } catch (Exception e) {
            log.debug("Source '{}' send await failed", this.name, e);
            return false;
        }
    }

    private static MessageRecord toMessage(OutboxRecord record) {
        return new MessageRecord()
                .setId(record.getId())
                .setType(record.getType())
                .setEventTime(record.getEventTime())
                .setTopic(record.getTopic())
                .setTenantId(record.getTenantId())
                .setBusinessKey(record.getBusinessKey())
                .setHeaders(record.getHeaders())
                .setPayload(record.getPayload() == null
                        ? null
                        : record.getPayload().getBytes(StandardCharsets.UTF_8));
    }

}
