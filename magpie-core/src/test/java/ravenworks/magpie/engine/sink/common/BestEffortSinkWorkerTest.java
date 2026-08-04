package ravenworks.magpie.engine.sink.common;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BestEffortSinkWorkerTest {

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchCommitsOffset() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        var worker = new BestEffortSinkWorker("b1", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertTrue(store.size() == 0);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failedRecordIsStoredThenRetriedSuccessfully() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.SUCCESS, SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        var worker = new BestEffortSinkWorker("b2", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            // offset 1 第一次失败落库，重试成功后再发送一次；计数单调递增，不受瞬态影响
            await().atMost(3, TimeUnit.SECONDS).until(() -> handler.countByOffset(1) == 2);
            assertEquals(0, store.size());
            assertEquals(1, consumer.lastCommit());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failedRetryReentersRetryingUntilSuccess() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE, SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        var worker = new BestEffortSinkWorker("b3", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 原始投递 1 次失败 + 重试 1 次失败 + 重试 1 次成功
            await().atMost(3, TimeUnit.SECONDS).until(() -> handler.countByOffset(0) == 3);
            assertEquals(0, store.size());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
