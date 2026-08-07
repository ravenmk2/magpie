package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.impl.sink.worker.SinkWorker;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderedDelivererTest {

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchAdvancesOffsetAndCommits() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new SinkWorker("w1", consumer, handler, closedCircuit(), null, 100, new OrderedDeliverer());
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
        var worker = new SinkWorker("w2", consumer, handler, closedCircuit(), null, 100, new OrderedDeliverer());
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
        var worker = new SinkWorker("w3", consumer, handler, closedCircuit(), null, 100, new OrderedDeliverer());
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
        var worker = new SinkWorker("w4", consumer, handler, circuitBreaker, null, 100, new OrderedDeliverer());
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
        var worker = new SinkWorker("w5", consumer, handler, closedCircuit(), null, 100, new OrderedDeliverer());
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
    void redeliveredOffsetsUpToLastCommitAreSkipped() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new SinkWorker("w6", consumer, handler, closedCircuit(), null, 100, new OrderedDeliverer());
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);

            // 重投包含已处理 offset（at-least-once 重启场景）：offset <= 已提交水位的记录被跳过
            consumer.offer(List.of(FakeStreamConsumer.record(1, "b"), FakeStreamConsumer.record(2, "c")));
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 2);
            assertEquals(1, handler.countByOffset(0));
            assertEquals(1, handler.countByOffset(1));
            assertEquals(1, handler.countByOffset(2));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
