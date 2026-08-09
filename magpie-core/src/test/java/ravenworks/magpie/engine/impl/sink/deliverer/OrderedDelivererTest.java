package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.impl.sink.SinkWorker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class OrderedDelivererTest {

    // 测试用短提交间隔：节流提交在 await 窗口内尽快生效
    private static final long COMMIT_INTERVAL = 50;

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchAdvancesOffsetAndCommits() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new SinkWorker("w1", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w1", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertTrue(consumer.started.get());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
        assertTrue(consumer.stopped.get());
        assertEquals(1, consumer.lastCommit());
    }

    @Test
    void backoffRetriesSameRecordInPlace() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.BACKOFF);
        var worker = new SinkWorker("w2", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w2", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 0);
            assertEquals(2, handler.countByOffset(0));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failureRetriesInPlaceWithoutSkip() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var worker = new SinkWorker("w3", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w3", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            // ORDERED 语义：FAILURE 不跳过——offset 0 原地重试成功后才处理 offset 1，
            // 失败期间不提交任何 offset
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertEquals(2, handler.countByOffset(0));
            assertEquals(0, handler.handledRecords.get(0).getOffset());
            assertEquals(0, handler.handledRecords.get(1).getOffset());
            assertEquals(1, handler.handledRecords.get(2).getOffset());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void openCircuitBreakerPausesProcessing() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var circuitBreaker = new CircuitBreaker("t", 1, 1, 10_000);
        circuitBreaker.recordFailure();
        var worker = new SinkWorker("w4", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w4", handler, circuitBreaker));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            Thread.sleep(250);
            assertTrue(handler.handledRecords.isEmpty());
            assertTrue(consumer.commits.isEmpty());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void handlerExceptionRetriesSameRecordInPlace() throws Exception {
        var consumer = new FakeStreamConsumer();
        var thrown = new AtomicBoolean(false);
        var handler = new FakeSinkHandler() {

            @Override
            public CompletableFuture<SinkResult> handle(ConsumerRecord record) {
                if (record.getOffset() == 0 && thrown.compareAndSet(false, true)) {
                    return CompletableFuture.failedFuture(new RuntimeException("simulated handler crash"));
                }
                return super.handle(record);
            }
        };
        var worker = new SinkWorker("w5", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w5", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            // handler 崩溃同样不跳过：offset 0 重试成功后才处理 offset 1
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertTrue(thrown.get());
            assertEquals(0, handler.handledRecords.get(0).getOffset());
            assertEquals(1, handler.handledRecords.get(1).getOffset());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void redeliveredOffsetsUpToLastSeenAreSkipped() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new SinkWorker("w6", consumer, 100, COMMIT_INTERVAL,
                new OrderedDeliverer("w6", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);

            // 重投包含已拉取过的 offset（at-least-once 重启场景）：offset <= lastOffset 的记录被跳过
            consumer.offer(List.of(FakeStreamConsumer.record(1, "b"), FakeStreamConsumer.record(2, "c")));
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 2);
            assertEquals(1, handler.countByOffset(0));
            assertEquals(1, handler.countByOffset(1));
            assertEquals(1, handler.countByOffset(2));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void commitIsThrottledByInterval() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new SinkWorker("w7", consumer, 100, 1_000,
                new OrderedDeliverer("w7", handler, closedCircuit()));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 处理完成后不立即提交：节流窗口内 commits 为空
            await().atMost(2, TimeUnit.SECONDS).until(() -> handler.countByOffset(0) == 1);
            Thread.sleep(200);
            assertTrue(consumer.commits.isEmpty());

            // 超过间隔后由周期提交兜底
            await().atMost(3, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 0);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void interruptedMidBatchReturnsDeliveredPrefix() {
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.SUCCESS, SinkStatus.INTERRUPTED);
        var deliverer = new OrderedDeliverer("w8", handler, closedCircuit());

        var outcome = deliverer.deliver(List.of(
                FakeStreamConsumer.record(0, "a"),
                FakeStreamConsumer.record(1, "b"),
                FakeStreamConsumer.record(2, "c")));

        // INTERRUPTED 走提前返回分支：水位停在已投递前缀（offset 0），completed=false 为中断信号
        assertEquals(0, outcome.watermark());
        assertFalse(outcome.completed());
        // 未处置后缀（offset 2）未被尝试投递
        assertEquals(List.of(0L, 1L),
                handler.handledRecords.stream().map(ConsumerRecord::getOffset).toList());
    }

    @Test
    void emptyBatchCompletesWithoutProgress() {
        var handler = new FakeSinkHandler();
        var deliverer = new OrderedDeliverer("w9", handler, closedCircuit());

        var outcome = deliverer.deliver(List.of());

        // 本批无进展：watermark=-1，但已处置完毕
        assertEquals(-1, outcome.watermark());
        assertTrue(outcome.completed());
        assertTrue(handler.handledRecords.isEmpty());
    }

}
