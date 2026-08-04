package ravenworks.magpie.engine.sink.common;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyOrderedSinkWorkerTest {

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchCommitsOffset() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        var worker = new KeyOrderedSinkWorker("k1", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertEquals(0, store.size());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failedRecordBlocksKeyAndDivertsFollowing() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        var worker = new KeyOrderedSinkWorker("k2", consumer, handler, closedCircuit(), store, 100);
        try {
            // 两批预先投放：第一批失败后 key=a 被阻塞，第二批同 key 消息应直接落库不发送
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            consumer.offer(List.of(FakeStreamConsumer.record(1, "a"), FakeStreamConsumer.record(2, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 2);

            // 等待 RETRYING 把 store 排空（默认 SUCCESS）到达稳定终态后断言：
            // offset 1 只经重试发送 1 次（正常路径被分流），证明 key 阻塞生效
            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    handler.countByOffset(0) == 2 && store.size() == 0);
            assertEquals(1, handler.countByOffset(1));
            assertEquals(1, handler.countByOffset(2));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void retryDrainsStoreAndUnblocksKey() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        var worker = new KeyOrderedSinkWorker("k3", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 空轮询后进入 RETRYING，重试成功（默认 SUCCESS）并清空 store；计数单调递增
            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    handler.countByOffset(0) == 2 && store.size() == 0);

            // key 已解除阻塞，后续同 key 消息正常发送
            consumer.offer(List.of(FakeStreamConsumer.record(3, "a")));
            await().atMost(2, TimeUnit.SECONDS).until(() -> handler.countByOffset(3) == 1);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failedRetriesKeepKeyBlockedUntilRetrySucceeds() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE, SinkStatus.FAILURE, SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        var worker = new KeyOrderedSinkWorker("k4", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            consumer.offer(List.of(FakeStreamConsumer.record(9, "a")));
            worker.start();

            // 原始失败 1 次 + 重试失败 2 次，期间 key 始终阻塞（offset 9 只在重试成功后被发送）
            await().atMost(3, TimeUnit.SECONDS).until(() ->
                    handler.countByOffset(0) == 4 && store.size() == 0);
            assertEquals(1, handler.countByOffset(9));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void startsInRetryingWhenStoreHasEntries() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        store.save("k5", FakeStreamConsumer.record(0, "a"));
        var worker = new KeyOrderedSinkWorker("k5", consumer, handler, closedCircuit(), store, 100);
        try {
            worker.start();

            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0);
            assertEquals(1, handler.countByOffset(0));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void subBatchesAreSplitByUniqueKey() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        var worker = new KeyOrderedSinkWorker("k6", consumer, handler, closedCircuit(), store, 100);
        try {
            consumer.offer(List.of(
                    FakeStreamConsumer.record(0, "a"),
                    FakeStreamConsumer.record(1, "b"),
                    FakeStreamConsumer.record(2, "a")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 2);
            assertEquals(2, handler.handledBatches.size());
            assertEquals(2, handler.handledBatches.get(0).size());
            assertEquals(1, handler.handledBatches.get(1).size());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
