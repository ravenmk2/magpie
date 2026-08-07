package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.StreamConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 按脚本返回 poll 批次、记录 commit/stop 的测试用 StreamConsumer。
 */
class FakeStreamConsumer implements StreamConsumer {

    private final Queue<List<ConsumerRecord>> scripted = new ConcurrentLinkedQueue<>();
    final AtomicBoolean started = new AtomicBoolean();
    final AtomicBoolean stopped = new AtomicBoolean();
    final List<Long> commits = new CopyOnWriteArrayList<>();

    void offer(List<ConsumerRecord> batch) {
        this.scripted.add(batch);
    }

    @Override
    public int partition() {
        return 0;
    }

    @Override
    public void start() {
        this.started.set(true);
    }

    @Override
    public void stop() {
        this.stopped.set(true);
    }

    @Override
    public List<ConsumerRecord> poll(int count, Duration timeout) {
        var batch = this.scripted.poll();
        return batch != null ? batch : List.of();
    }

    @Override
    public void commit(long offset) {
        this.commits.add(offset);
    }

    long lastCommit() {
        return this.commits.isEmpty() ? -1 : this.commits.get(this.commits.size() - 1);
    }

    static ConsumerRecord record(long offset, String businessKey) {
        return new ConsumerRecord()
                .setOffset(offset)
                .setId("msg-" + offset)
                .setType("t.test")
                .setTopic("topic")
                .setEventTime(LocalDateTime.now())
                .setBusinessKey(businessKey);
    }

}
