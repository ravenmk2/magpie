package ravenworks.magpie.engine.retry;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.domain.entity.MessageLogEntity;
import ravenworks.magpie.domain.entity.RetryMessageEntity;
import ravenworks.magpie.domain.repository.MessageLogRepository;
import ravenworks.magpie.domain.repository.RetryMessageRepository;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
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
        var entities = this.retryMessageRepository.findByConsumerOrderByIdAsc(consumer, PageRequest.of(0, count));
        return toRetryRecords(entities);
    }

    @Override
    public List<RetryRecord> listRetryable(@NonNull String consumer, int count) {
        var entities = this.retryMessageRepository.findByConsumerAndRetryAtBeforeOrderByIdAsc(
                consumer, LocalDateTime.now(), PageRequest.of(0, count));
        return toRetryRecords(entities);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void save(@NonNull String consumer, @NonNull ConsumerRecord record) {
        var logEntity = new MessageLogEntity();
        logEntity.setId(Uuids.uuid7Hex());
        logEntity.setMessageId(record.getId());
        logEntity.setType(record.getType());
        logEntity.setEventTime(record.getEventTime());
        logEntity.setTopic(record.getTopic());
        logEntity.setTenantId(record.getTenantId());
        logEntity.setBusinessKey(record.getBusinessKey());
        logEntity.setHeaders(record.getHeaders());
        logEntity.setPayload(Base64.getEncoder().encodeToString(record.getPayload()));
        this.messageLogRepository.save(logEntity);

        var retryEntity = new RetryMessageEntity();
        retryEntity.setId(Uuids.uuid7Hex());
        retryEntity.setConsumer(consumer);
        retryEntity.setLogId(logEntity.getId());
        retryEntity.setAttempts(0);
        retryEntity.setRetryAt(LocalDateTime.now());
        retryEntity.setBusinessKey(record.getBusinessKey());
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
