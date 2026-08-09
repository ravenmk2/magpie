package ravenworks.magpie.testsupport;

import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;


/**
 * 录制型 SinkHandler（IT 用）：记录所有经手消息，支持动态失败规则。
 * 命中失败规则的 record 返回 FAILURE，其余 SUCCESS；规则可随时改（volatile），
 * 用于模拟下游故障与恢复。参考 deliverer 单测里的 FakeSinkHandler（包私有，无法复用）。
 *
 * <p>刻意简化：不向 CircuitBreaker 记录成败，熔断永不开启，BACKOFF 路径不在此覆盖。
 */
public class RecordingSinkHandler implements SinkHandler {

    private final List<ConsumerRecord> received = new CopyOnWriteArrayList<>();
    private volatile Predicate<ConsumerRecord> failureRule = r -> false;

    /**
     * 命中谓词的 record 一律返回 FAILURE（直到 {@link #clearFailures()}）。
     */
    public void failWhen(Predicate<ConsumerRecord> rule) {
        this.failureRule = rule;
    }

    public void clearFailures() {
        this.failureRule = r -> false;
    }

    /**
     * 按收到顺序的快照。含失败/重试的重复尝试——ORDERED 原地重试与
     * RetryStore 重投都会再次经手同一 record，断言时按需去重。
     */
    public List<ConsumerRecord> received() {
        return List.copyOf(this.received);
    }

    /**
     * 收到消息的 payload（UTF-8 解码），与 {@link #received()} 同序。
     */
    public List<String> receivedPayloads() {
        return this.received().stream()
                .map(r -> new String(r.getMessage().getPayload(), StandardCharsets.UTF_8))
                .toList();
    }

    @Override
    public CompletableFuture<SinkResult> handle(ConsumerRecord record) {
        this.received.add(record);
        return CompletableFuture.completedFuture(this.resultFor(record));
    }

    @Override
    public CompletableFuture<List<SinkResult>> handle(List<ConsumerRecord> records) {
        this.received.addAll(records);
        return CompletableFuture.completedFuture(records.stream().map(this::resultFor).toList());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

    private SinkResult resultFor(ConsumerRecord record) {
        boolean failed = this.failureRule.test(record);
        return new SinkResult()
                .setStatus(failed ? SinkStatus.FAILURE : SinkStatus.SUCCESS)
                .setAttempts(1)
                .setError(failed ? "recording sink forced failure" : null)
                .setRecord(record);
    }

}
