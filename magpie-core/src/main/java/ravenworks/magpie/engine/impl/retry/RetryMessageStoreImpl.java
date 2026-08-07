package ravenworks.magpie.engine.impl.retry;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.domain.entity.MessageLogEntity;
import ravenworks.magpie.domain.entity.RetryMessageEntity;
import ravenworks.magpie.domain.repository.MessageLogRepository;
import ravenworks.magpie.domain.repository.RetryMessageRepository;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class RetryMessageStoreImpl implements RetryMessageStore {

    /**
     * 首次重试延迟；之后按 attempts 指数退避，封顶 RETRY_MAX_DELAY_MS，无次数上限（无 DLQ）
     */
    private static final long RETRY_DELAY_MS = 5_000;
    private static final long RETRY_MAX_DELAY_MS = 300_000;

    private final MessageLogRepository messageLogRepository;
    private final RetryMessageRepository retryMessageRepository;

    public RetryMessageStoreImpl(@NonNull MessageLogRepository messageLogRepository,
                                 @NonNull RetryMessageRepository retryMessageRepository) {
        this.messageLogRepository = messageLogRepository;
        this.retryMessageRepository = retryMessageRepository;
    }

    @Override
    public Set<String> listKeys(@NonNull String consumer) {
        return this.retryMessageRepository.findDistinctBusinessKeysByConsumer(consumer);
    }

    @Override
    public List<RetryRecord> list(@NonNull String consumer, int count) {
        var entities = this.retryMessageRepository.findByConsumerOrderByOffsetAsc(consumer, PageRequest.of(0, count));
        return toRetryRecords(entities);
    }

    @Override
    public List<RetryRecord> listRetryable(@NonNull String consumer, int count) {
        var entities = this.retryMessageRepository.findByConsumerAndRetryAtBeforeOrderByOffsetAsc(
                consumer, LocalDateTime.now(), PageRequest.of(0, count));
        return toRetryRecords(entities);
    }

    /**
     * 落库前对记录做归一化：数据库列均为 NOT NULL（message_id 为 CHAR(32)），
     * 任何空值/超长都不能让重试落库本身失败——那会把安全网变成毒消息源。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void save(@NonNull String consumer, @NonNull ConsumerRecord record) {
        var logEntity = new MessageLogEntity();
        logEntity.setId(Uuids.uuid7Hex());
        logEntity.setMessageId(normalizeMessageId(record.getId()));
        logEntity.setType(nullToEmpty(record.getType()));
        logEntity.setEventTime(record.getEventTime() != null ? record.getEventTime() : LocalDateTime.now());
        logEntity.setTopic(nullToEmpty(record.getTopic()));
        logEntity.setTenantId(nullToEmpty(record.getTenantId()));
        logEntity.setBusinessKey(nullToEmpty(record.getBusinessKey()));
        logEntity.setHeaders(record.getHeaders() != null ? record.getHeaders() : Map.of());
        logEntity.setPayload(record.getPayload() != null
                ? Base64.getEncoder().encodeToString(record.getPayload())
                : "");
        this.messageLogRepository.save(logEntity);

        var retryEntity = new RetryMessageEntity();
        retryEntity.setId(Uuids.uuid7Hex());
        retryEntity.setConsumer(consumer);
        retryEntity.setLogId(logEntity.getId());
        retryEntity.setOffset(record.getOffset());
        retryEntity.setAttempts(0);
        retryEntity.setBusinessKey(nullToEmpty(record.getBusinessKey()));
        // 同 key 顺序不变式: 新条目的可重试时间不早于同 key 更老条目,
        // 避免老条目退避期间新条目抢先重试破坏 key 内 offset 顺序
        var now = LocalDateTime.now();
        var heldUntil = this.retryMessageRepository.findMaxRetryAtOfOlderSameKey(
                consumer, retryEntity.getBusinessKey(), record.getOffset());
        retryEntity.setRetryAt(heldUntil != null && heldUntil.isAfter(now) ? heldUntil : now);
        this.retryMessageRepository.save(retryEntity);

        log.info("[{}] saved retry message, id={}, businessKey={}", consumer, retryEntity.getId(), record.getBusinessKey());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void succeeded(@NonNull String id) {
        this.retryMessageRepository.deleteById(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void failed(@NonNull String id) {
        var entity = this.retryMessageRepository.findById(id).orElse(null);
        if (entity == null) {
            log.warn("Retry message not found: {}", id);
            return;
        }
        entity.setAttempts(entity.getAttempts() + 1);
        entity.setRetryAt(LocalDateTime.now().plus(Duration.ofMillis(computeRetryDelayMs(entity.getAttempts()))));
        this.retryMessageRepository.save(entity);
        // 同 key 顺序不变式: 本条退避期间，更晚的同 key 条目不得先于本条重试
        this.retryMessageRepository.pushBackLaterSameKey(
                entity.getConsumer(), entity.getBusinessKey(), entity.getOffset(), entity.getRetryAt());
    }

    /**
     * 退避时长：RETRY_DELAY_MS * 2^(attempts-1)，封顶 RETRY_MAX_DELAY_MS。
     * long 移位按 64 取模且乘法可能溢出，统一提前封顶。
     */
    static long computeRetryDelayMs(int attempts) {
        int shift = Math.min(Math.max(attempts - 1, 0), 62);
        long multiplier = 1L << shift;
        if (RETRY_DELAY_MS > Long.MAX_VALUE / multiplier) {
            return RETRY_MAX_DELAY_MS;
        }
        return Math.min(RETRY_DELAY_MS * multiplier, RETRY_MAX_DELAY_MS);
    }

    /**
     * message_id 约定为 32 字符 uuid7 hex：缺失时生成新 id，超长时截断，
     * 保证总能落入 magpie_message_log.message_id (CHAR(32))。
     */
    static String normalizeMessageId(String id) {
        if (id == null || id.isBlank()) {
            return Uuids.uuid7Hex();
        }
        return id.length() <= 32 ? id : id.substring(0, 32);
    }

    static String nullToEmpty(String value) {
        return value != null ? value : "";
    }

    private List<RetryRecord> toRetryRecords(@NonNull List<RetryMessageEntity> retryEntities) {
        if (retryEntities.isEmpty()) {
            return List.of();
        }
        Set<String> logIds = retryEntities.stream()
                .map(RetryMessageEntity::getLogId)
                .collect(Collectors.toSet());
        var logMap = this.messageLogRepository.findAllById(logIds).stream()
                .collect(Collectors.toMap(MessageLogEntity::getId, Function.identity()));

        List<RetryRecord> records = new ArrayList<>(retryEntities.size());
        for (var retry : retryEntities) {
            var messageLog = logMap.get(retry.getLogId());
            if (messageLog == null) {
                log.warn("Message log not found for retry: {}", retry.getId());
                continue;
            }
            records.add(buildRetryRecord(retry, messageLog));
        }
        return records;
    }

    private RetryRecord buildRetryRecord(@NonNull RetryMessageEntity retry,
                                         @NonNull MessageLogEntity log) {
        return new RetryRecord()
                .setId(retry.getId())
                .setLogId(log.getId())
                .setOffset(retry.getOffset())
                .setMessageId(log.getMessageId())
                .setType(log.getType())
                .setEventTime(log.getEventTime())
                .setTopic(log.getTopic())
                .setTenantId(log.getTenantId())
                .setBusinessKey(log.getBusinessKey())
                .setHeaders(log.getHeaders())
                .setPayload(Base64.getDecoder().decode(log.getPayload()))
                .setAttempts(retry.getAttempts())
                .setRetryAt(retry.getRetryAt());
    }

}
