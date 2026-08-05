package ravenworks.magpie.engine.impl.sink.worker;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkStatus;

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

    @Test
    void storeOutageStallsSaveButDoesNotLoseMessage() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        store.saveFailures.set(1); // 第一次落库抛异常, saveWithRetry 每秒重试
        var worker = new BestEffortSinkWorker("b4", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 存储瞬断约 1s: saveWithRetry 原地重试, 恢复后落库、提交并重试成功;
            // 直接等终态——落库成功到重试排空之间窗口极短(fake 空轮询不阻塞), 中间态断言会抖
            await().atMost(8, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            assertEquals(0, store.saveFailures.get()); // 故障确实发生并被重试消耗
            assertEquals(0, consumer.lastCommit());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
