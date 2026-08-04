package ravenworks.magpie.engine.sink.common;

import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.retry.RetryRecord;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 内存版 RetryMessageStore，忽略 consumer 维度与 retryAt 调度。
 */
class InMemoryRetryMessageStore implements RetryMessageStore {

    private final List<RetryRecord> records = new CopyOnWriteArrayList<>();
    private final AtomicInteger idSeq = new AtomicInteger();

    @Override
    public Set<String> listKeys(String consumer) {
        return this.records.stream()
                .map(RetryRecord::getBusinessKey)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public List<RetryRecord> list(String consumer, int count) {
        return this.records.stream().limit(count).toList();
    }

    @Override
    public List<RetryRecord> listRetryable(String consumer, int count) {
        return this.records.stream().limit(count).toList();
    }

    @Override
    public void save(String consumer, ConsumerRecord record) {
        this.records.add(new RetryRecord()
                .setId("retry-" + this.idSeq.incrementAndGet())
                .setOffset(record.getOffset())
                .setMessageId(record.getId())
                .setType(record.getType())
                .setEventTime(record.getEventTime())
                .setTopic(record.getTopic())
                .setTenantId(record.getTenantId())
                .setBusinessKey(record.getBusinessKey())
                .setHeaders(record.getHeaders())
                .setPayload(record.getPayload()));
    }

    @Override
    public void succeeded(String id) {
        this.records.removeIf(r -> r.getId().equals(id));
    }

    @Override
    public void failed(String id, LocalDateTime retryAt) {
        this.records.stream()
                .filter(r -> r.getId().equals(id))
                .forEach(r -> r.setAttempts(r.getAttempts() + 1));
    }

    int size() {
        return this.records.size();
    }

    List<RetryRecord> records() {
        return this.records;
    }

}
