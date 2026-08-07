package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * 按脚本返回结果（默认 SUCCESS）、记录所有经手消息的测试用 SinkHandler。
 */
class FakeSinkHandler implements SinkHandler {

    private final Queue<SinkStatus> script = new ConcurrentLinkedQueue<>();
    final List<ConsumerRecord> handledRecords = new CopyOnWriteArrayList<>();
    final List<List<ConsumerRecord>> handledBatches = new CopyOnWriteArrayList<>();

    void thenReturn(SinkStatus... statuses) {
        this.script.addAll(List.of(statuses));
    }

    @Override
    public CompletableFuture<SinkResult> handle(ConsumerRecord record) {
        this.handledRecords.add(record);
        return CompletableFuture.completedFuture(new SinkResult()
                .setStatus(nextStatus())
                .setAttempts(1)
                .setRecord(record));
    }

    @Override
    public CompletableFuture<List<SinkResult>> handle(List<ConsumerRecord> records) {
        this.handledBatches.add(records);
        this.handledRecords.addAll(records);
        var results = records.stream()
                .map(r -> new SinkResult().setStatus(nextStatus()).setAttempts(1).setRecord(r))
                .toList();
        return CompletableFuture.completedFuture(results);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

    long countByOffset(long offset) {
        return this.handledRecords.stream().filter(r -> r.getOffset() == offset).count();
    }

    private SinkStatus nextStatus() {
        var status = this.script.poll();
        return status != null ? status : SinkStatus.SUCCESS;
    }

}
