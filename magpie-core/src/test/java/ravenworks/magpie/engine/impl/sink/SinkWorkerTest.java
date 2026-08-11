package ravenworks.magpie.engine.impl.sink;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.impl.sink.deliverer.BatchOutcome;
import ravenworks.magpie.engine.impl.sink.deliverer.Deliverer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * SinkWorker 工作循环骨架测试：真实 WorkLoop 线程 + 手写 Fake（不经 Deliverer 实现类），
 * 覆盖会话内去重（filterByLastOffset 只认批次首条）、按 commitInterval 节流的提交、
 * 中断信号（completed=false）立即提交并停拉、熔断未就绪停顿与空转时的重试排空。
 */
class SinkWorkerTest {

    static class FakeStreamConsumer implements StreamConsumer {

        private final Queue<List<ConsumerRecord>> scripted = new ConcurrentLinkedQueue<>();
        final AtomicBoolean started = new AtomicBoolean();
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicBoolean failFatally = new AtomicBoolean();
        final AtomicInteger pollCount = new AtomicInteger();
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
            this.pollCount.incrementAndGet();
            if (this.failFatally.get()) {
                throw new AssertionError("simulated fatal poll failure");
            }
            var batch = this.scripted.poll();
            return batch != null ? batch : List.of();
        }

        @Override
        public void commit(long offset) {
            this.commits.add(offset);
        }

    }


    static class FakeDeliverer implements Deliverer {

        final AtomicBoolean ready = new AtomicBoolean(true);
        final AtomicBoolean retryable = new AtomicBoolean(false);
        final AtomicInteger retryCount = new AtomicInteger();
        final AtomicInteger interruptCount = new AtomicInteger();
        final List<List<ConsumerRecord>> delivered = new CopyOnWriteArrayList<>();
        private final Queue<BatchOutcome> script = new ConcurrentLinkedQueue<>();

        void thenReturn(BatchOutcome... outcomes) {
            this.script.addAll(List.of(outcomes));
        }

        @Override
        public boolean isReady() {
            return this.ready.get();
        }

        @Override
        public boolean canRetry() {
            return this.retryable.get();
        }

        @Override
        public BatchOutcome deliver(List<ConsumerRecord> records) {
            this.delivered.add(records);
            var outcome = this.script.poll();
            if (outcome != null) {
                return outcome;
            }
            // 默认全量处置完毕：水位即批次末条 offset
            return new BatchOutcome(records.getLast().getOffset(), true);
        }

        @Override
        public void retry() {
            this.retryCount.incrementAndGet();
        }

        @Override
        public void interrupt() {
            this.interruptCount.incrementAndGet();
        }

    }


    static class Harness {

        final FakeStreamConsumer consumer = new FakeStreamConsumer();
        final FakeDeliverer deliverer = new FakeDeliverer();
        final SinkWorker worker;

        Harness(long commitInterval) {
            this.worker = new SinkWorker("t", this.consumer, 10, commitInterval, this.deliverer);
        }

        void awaitDelivered(int batches) {
            await().atMost(2, TimeUnit.SECONDS).until(() -> this.deliverer.delivered.size() == batches);
        }

        void shutdown() throws Exception {
            this.worker.shutdown().get(2, TimeUnit.SECONDS);
        }

    }

    static ConsumerRecord record(long offset) {
        var message = new MessageRecord()
                .setId("msg-" + offset)
                .setType("t.test")
                .setTopic("topic")
                .setEventTime(LocalDateTime.now())
                .setBusinessKey("key-" + offset);
        return new ConsumerRecord()
                .setOffset(offset)
                .setMessage(message);
    }

    static List<Long> offsets(List<ConsumerRecord> batch) {
        return batch.stream().map(ConsumerRecord::getOffset).toList();
    }

    @Test
    void firstBatchPassesThroughWhenLastOffsetNotSet() throws Exception {
        var h = new Harness(60_000);
        try {
            h.worker.start();
            // lastOffset 尚未推进（<0）：批次原样交给 Deliverer
            h.consumer.offer(List.of(record(0), record(1), record(2)));
            h.awaitDelivered(1);

            assertEquals(List.of(0L, 1L, 2L), offsets(h.deliverer.delivered.getFirst()));
        } finally {
            h.shutdown();
        }
    }

    @Test
    void duplicateAtBatchHeadIsFilteredByLastOffset() throws Exception {
        var h = new Harness(60_000);
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1), record(2)));
            h.awaitDelivered(1);

            // 重放批次首部命中 lastOffset：过滤掉 offset <= 2 的前缀
            h.consumer.offer(List.of(record(1), record(2), record(3)));
            h.awaitDelivered(2);

            assertEquals(List.of(3L), offsets(h.deliverer.delivered.get(1)));
        } finally {
            h.shutdown();
        }
    }

    @Test
    void midBatchDuplicatePassesThroughWhenHeadIsNew() throws Exception {
        var h = new Harness(60_000);
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1)));
            h.awaitDelivered(1);

            // 首条合格即全部合格（生产假定批次按 offset 有序）：
            // 乱序混入批次中部的重放记录不会被过滤，原样投递
            h.consumer.offer(List.of(record(5), record(0), record(6)));
            h.awaitDelivered(2);

            assertEquals(List.of(5L, 0L, 6L), offsets(h.deliverer.delivered.get(1)));
        } finally {
            h.shutdown();
        }
    }

    @Test
    void commitIsThrottledBeforeIntervalElapses() throws Exception {
        var h = new Harness(60_000);
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1), record(2)));
            h.awaitDelivered(1);

            // 距启动（lastCommitAt）不足 commitInterval：有进展也不提交
            Thread.sleep(250);
            assertTrue(h.consumer.commits.isEmpty());
        } finally {
            h.shutdown();
        }
    }

    @Test
    void commitFiresOnceAfterIntervalAndAgainOnNewProgress() throws Exception {
        var h = new Harness(50);
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1), record(2)));
            // 超过 commitInterval：提交一次已处置水位
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.consumer.commits.equals(List.of(2L)));

            // 无新进展（committable <= committed）：不再重复提交
            Thread.sleep(200);
            assertEquals(List.of(2L), h.consumer.commits);

            // 新进展推进水位后再次按节流提交
            h.consumer.offer(List.of(record(3), record(4)));
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.consumer.commits.equals(List.of(2L, 4L)));
        } finally {
            h.shutdown();
        }
    }

    @Test
    void interruptSignalCommitsWatermarkAndHaltsPolling() throws Exception {
        var h = new Harness(60_000);
        // 批次 [0,1,2] 只处置到 offset 0 即中断
        h.deliverer.thenReturn(new BatchOutcome(0, false));
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1), record(2)));

            // 中断信号：立即提交已处置前缀水位，不等 commitInterval
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.consumer.commits.equals(List.of(0L)));

            int pollsAtHalt = h.consumer.pollCount.get();
            h.consumer.offer(List.of(record(3), record(4)));
            Thread.sleep(300);

            // 已 halted：不再拉取、不再投递，未处置后缀等重启重投
            assertEquals(pollsAtHalt, h.consumer.pollCount.get());
            assertEquals(1, h.deliverer.delivered.size());
        } finally {
            h.shutdown();
        }
    }

    @Test
    void notReadyDelivererPausesPollingUntilReady() throws Exception {
        var h = new Harness(60_000);
        h.deliverer.ready.set(false);
        try {
            h.worker.start();
            h.consumer.offer(List.of(record(0), record(1)));

            // 熔断开启（未就绪）：停顿再探，始终不拉取
            Thread.sleep(450);
            assertEquals(0, h.consumer.pollCount.get());
            assertTrue(h.deliverer.delivered.isEmpty());

            // 恢复就绪：拉取与投递随之恢复
            h.deliverer.ready.set(true);
            h.awaitDelivered(1);
            assertEquals(List.of(0L, 1L), offsets(h.deliverer.delivered.getFirst()));
        } finally {
            h.shutdown();
        }
    }

    @Test
    void shutdownAfterLoopDiedStillStopsConsumer() throws Exception {
        var h = new Harness(60_000);
        h.worker.start();
        await().atMost(2, TimeUnit.SECONDS).until(() -> h.consumer.pollCount.get() > 0);
        assertTrue(h.worker.isAlive());

        // 循环线程被 Error 杀死：PRE_SHUTDOWN 不会执行，isAlive 随之翻转为 false
        h.consumer.failFatally.set(true);
        await().atMost(2, TimeUnit.SECONDS).until(() -> !h.worker.isAlive());

        // shutdown 仍须兜底停掉 consumer（避免底层订阅挂在死线程上泄漏），
        // 且停止一个已死 worker 是干净的成功路径（future 正常完成）
        h.worker.shutdown().get(2, TimeUnit.SECONDS);
        assertTrue(h.consumer.stopped.get());
    }

    @Test
    void emptyPollDrainsRetriesOnlyWhenDelivererCanRetry() throws Exception {
        var h = new Harness(60_000);
        try {
            h.worker.start();

            // 空批次且 canRetry=false：不触发重试排空
            Thread.sleep(250);
            assertEquals(0, h.deliverer.retryCount.get());

            // canRetry=true：空闲窗口驱动重试排空
            h.deliverer.retryable.set(true);
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.deliverer.retryCount.get() > 0);
        } finally {
            h.shutdown();
        }
    }

}
