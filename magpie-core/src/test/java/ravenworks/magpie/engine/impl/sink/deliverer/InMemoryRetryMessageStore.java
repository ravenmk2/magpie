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
        // 与 JDBC 实现一致: businessKey 归一化为 "", 且同 key 新条目的
        // 可重试时间不早于更老条目(维持 key 内 offset 顺序)
        String key = record.getBusinessKey() != null ? record.getBusinessKey() : "";
        var now = LocalDateTime.now();
        LocalDateTime retryAt = null;
        for (var r : this.records) {
            if (Objects.equals(r.getBusinessKey(), key) && r.getOffset() < record.getOffset()
                    && r.getRetryAt() != null && r.getRetryAt().isAfter(now)
                    && (retryAt == null || r.getRetryAt().isAfter(retryAt))) {
                retryAt = r.getRetryAt();
            }
        }
        this.records.add(new RetryRecord()
                .setId("retry-" + this.idSeq.incrementAndGet())
                .setOffset(record.getOffset())
                .setMessageId(record.getId())
                .setType(record.getType())
                .setEventTime(record.getEventTime())
                .setTopic(record.getTopic())
                .setTenantId(record.getTenantId())
                .setBusinessKey(key)
                .setHeaders(record.getHeaders())
                .setPayload(record.getPayload())
                .setRetryAt(retryAt));
    }

    @Override
    public void succeeded(String id) {
        this.records.removeIf(r -> r.getId().equals(id));
    }

    /**
     * 测试语义：失败立即可重试（retryAt=now），不退避，保持用例快节奏。
     */
    @Override
    public void failed(String id) {
        this.records.stream()
                .filter(r -> r.getId().equals(id))
                .forEach(r -> {
                    r.setAttempts(r.getAttempts() + 1);
                    r.setRetryAt(LocalDateTime.now());
                    pushBackLaterSameKey(r);
                });
    }

    // 与 JDBC 实现一致: head 退避时, 更晚的同 key 条目的可重试时间不早于 head
    private void pushBackLaterSameKey(RetryRecord head) {
        var now = LocalDateTime.now();
        for (var r : this.records) {
            if (Objects.equals(r.getBusinessKey(), head.getBusinessKey())
                    && r.getOffset() > head.getOffset()) {
                var effective = r.getRetryAt() != null ? r.getRetryAt() : now;
                if (head.getRetryAt().isAfter(effective)) {
                    r.setRetryAt(head.getRetryAt());
                }
            }
        }
    }

    int size() {
        return this.records.size();
    }

    List<RetryRecord> records() {
        return this.records;
    }

}
