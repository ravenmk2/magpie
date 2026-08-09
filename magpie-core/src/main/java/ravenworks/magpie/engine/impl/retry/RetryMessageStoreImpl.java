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

import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class RetryMessageStoreImpl implements RetryMessageStore {

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
        return this.toRetryRecords(entities);
    }

    @Override
    public List<RetryRecord> listRetryable(@NonNull String consumer, int count) {
        var entities = this.retryMessageRepository.findByConsumerAndRetryAtBeforeOrderByOffsetAsc(
                consumer, LocalDateTime.now(), PageRequest.of(0, count));
        return this.toRetryRecords(entities);
    }

    /**
     * 落库前对记录做归一化：数据库列均为 NOT NULL（message_id 为 CHAR(32)），
     * 任何空值/超长都不能让重试落库本身失败——那会把安全网变成毒消息源。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void save(@NonNull String consumer, @NonNull ConsumerRecord record) {
        var message = record.getMessage();
        var logEntity = new MessageLogEntity();
        logEntity.setId(Uuids.uuid7Hex());
        logEntity.setMessageId(normalizeMessageId(message.getId()));
        logEntity.setType(nullToEmpty(message.getType()));
        logEntity.setEventTime(message.getEventTime() != null ? message.getEventTime() : LocalDateTime.now());
        logEntity.setTopic(nullToEmpty(message.getTopic()));
        logEntity.setTenantId(nullToEmpty(message.getTenantId()));
        logEntity.setBusinessKey(nullToEmpty(message.getBusinessKey()));
        logEntity.setHeaders(message.getHeaders() != null ? message.getHeaders() : Map.of());
        logEntity.setPayload(message.getPayload() != null
                ? Base64.getEncoder().encodeToString(message.getPayload())
                : "");
        this.messageLogRepository.save(logEntity);

        var retryEntity = new RetryMessageEntity();
        retryEntity.setId(Uuids.uuid7Hex());
        retryEntity.setConsumer(consumer);
        retryEntity.setLogId(logEntity.getId());
        retryEntity.setOffset(record.getOffset());
        retryEntity.setAttempts(0);
        retryEntity.setBusinessKey(nullToEmpty(message.getBusinessKey()));
        // 新落库条目立即到期（首个空闲窗口即重试）；退避只发生在 failed 之后
        retryEntity.setRetryAt(LocalDateTime.now());
        this.retryMessageRepository.save(retryEntity);

        log.info("[{}] saved retry message, id={}, businessKey={}", consumer, retryEntity.getId(), message.getBusinessKey());
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void succeeded(@NonNull String id) {
        this.retryMessageRepository.deleteById(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void failed(@NonNull String id, @NonNull LocalDateTime retryAt) {
        var entity = this.retryMessageRepository.findById(id).orElse(null);
        if (entity == null) {
            log.warn("Retry message not found: {}", id);
            return;
        }
        entity.setAttempts(entity.getAttempts() + 1);
        entity.setRetryAt(retryAt);
        this.retryMessageRepository.save(entity);
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
            records.add(this.buildRetryRecord(retry, messageLog));
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
