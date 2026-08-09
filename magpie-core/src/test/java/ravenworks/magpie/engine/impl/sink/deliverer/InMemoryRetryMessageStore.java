package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * 内存版 RetryMessageStore：忽略 consumer 维度；retryAt 为 null 或不晚于当前时间视为到期。
 * 与生产语义一致：save 立即到期，failed 的可重试时间由调用方（Deliverer）传入。
 */
class InMemoryRetryMessageStore implements RetryMessageStore {

    private final List<RetryRecord> records = new CopyOnWriteArrayList<>();
    private final AtomicInteger idSeq = new AtomicInteger();
    /**
     * >0 时 save 抛异常并递减，模拟存储瞬断
     */
    final AtomicInteger saveFailures = new AtomicInteger();
    /**
     * true 时 save 一律抛异常，模拟存储持续故障
     */
    final AtomicBoolean failSaves = new AtomicBoolean();

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
        var now = LocalDateTime.now();
        return this.records.stream()
                .filter(r -> r.getRetryAt() == null || !r.getRetryAt().isAfter(now))
                .limit(count).toList();
    }

    @Override
    public void save(String consumer, ConsumerRecord record) {
        if (this.failSaves.get()) {
            throw new RuntimeException("simulated store outage");
        }
        if (this.saveFailures.getAndUpdate(n -> n > 0 ? n - 1 : n) > 0) {
            throw new RuntimeException("simulated store outage");
        }
        // 与 JDBC 实现一致: businessKey 归一化为 "", 新条目立即到期
        var message = record.getMessage();
        String key = message.getBusinessKey() != null ? message.getBusinessKey() : "";
        this.records.add(new RetryRecord()
                .setId("retry-" + this.idSeq.incrementAndGet())
                .setOffset(record.getOffset())
                .setMessageId(message.getId())
                .setType(message.getType())
                .setEventTime(message.getEventTime())
                .setTopic(message.getTopic())
                .setTenantId(message.getTenantId())
                .setBusinessKey(key)
                .setHeaders(message.getHeaders())
                .setPayload(message.getPayload())
                .setRetryAt(LocalDateTime.now()));
    }

    @Override
    public void succeeded(String id) {
        this.records.removeIf(r -> r.getId().equals(id));
    }

    @Override
    public void failed(String id, LocalDateTime retryAt) {
        this.records.stream()
                .filter(r -> r.getId().equals(id))
                .forEach(r -> {
                    r.setAttempts(r.getAttempts() + 1);
                    r.setRetryAt(retryAt);
                });
    }

    int size() {
        return this.records.size();
    }

    List<RetryRecord> records() {
        return this.records;
    }

}
