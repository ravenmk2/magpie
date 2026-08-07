package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.impl.sink.worker.SinkWorker;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class KeyOrderedDelivererTest {

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchCommitsOffset() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        var worker = new SinkWorker("k1", consumer, 100,
                new KeyOrderedDeliverer("k1", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("k2", consumer, 100,
                new KeyOrderedDeliverer("k2", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("k3", consumer, 100,
                new KeyOrderedDeliverer("k3", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("k4", consumer, 100,
                new KeyOrderedDeliverer("k4", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("k5", consumer, 100,
                new KeyOrderedDeliverer("k5", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("k6", consumer, 100,
                new KeyOrderedDeliverer("k6", handler, 100, closedCircuit(), store));
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

    @Test
    void backedOffRetryEntryWaitsUntilDue() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        store.records().add(new RetryRecord()
                .setId("r1")
                .setOffset(0)
                .setMessageId("m0")
                .setType("t.test")
                .setEventTime(LocalDateTime.now())
                .setTopic("topic")
                .setBusinessKey("a")
                .setRetryAt(LocalDateTime.now().plusHours(1)));
        var worker = new SinkWorker("k7", consumer, 100,
                new KeyOrderedDeliverer("k7", handler, 100, closedCircuit(), store));
        try {
            worker.start();

            // 未到期：不产生任何投递尝试，记录保留在库中（hasRetryable 保持 true）
            Thread.sleep(300);
            assertTrue(handler.handledRecords.isEmpty());
            assertEquals(1, store.size());

            // 到期后：自动重试成功并清除
            store.records().get(0).setRetryAt(LocalDateTime.now().minusSeconds(1));
            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0);
            assertEquals(1, handler.countByOffset(0));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void backedOffSameKeyEntryHoldsBackNewerOne() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        store.save("k8", FakeStreamConsumer.record(0, "a"));
        store.records().get(0).setRetryAt(LocalDateTime.now().plusHours(1)); // 模拟 offset 0 退避中
        var worker = new SinkWorker("k8", consumer, 100,
                new KeyOrderedDeliverer("k8", handler, 100, closedCircuit(), store));
        try {
            worker.start();
            consumer.offer(List.of(FakeStreamConsumer.record(1, "a"))); // 同 key: 分流后 retryAt 被推到 offset 0 之后
            consumer.offer(List.of(FakeStreamConsumer.record(2, "b"))); // 其他 key 不受影响

            await().atMost(2, TimeUnit.SECONDS).until(() -> handler.countByOffset(2) == 1);
            Thread.sleep(300);
            // 退避期间: 同 key 的 offset 1 不得越过 offset 0 投递
            assertEquals(0, handler.countByOffset(0));
            assertEquals(0, handler.countByOffset(1));

            // 到期后按 key 内 offset 顺序投递
            store.records().forEach(r -> r.setRetryAt(LocalDateTime.now().minusSeconds(1)));
            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0);
            var delivered = handler.handledRecords.stream().map(ConsumerRecord::getOffset).toList();
            assertEquals(List.of(2L, 0L, 1L), delivered);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void nullKeyRecordsAreOrderedAsEmptyKey() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE); // 仅第一次投递失败
        var store = new InMemoryRetryMessageStore();
        var worker = new SinkWorker("k9", consumer, 100,
                new KeyOrderedDeliverer("k9", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, null), FakeStreamConsumer.record(1, null)));
            worker.start();

            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            // offset 0 失败后 key("") 阻塞: offset 1 不越过它直发, 等重试按序到达
            var delivered = handler.handledRecords.stream().map(ConsumerRecord::getOffset).toList();
            assertEquals(List.of(0L, 0L, 1L), delivered);
            assertEquals(1, consumer.lastCommit());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void shutdownCommitDoesNotSkipUnpersistedFailure() throws Exception {
        var consumer = new FakeStreamConsumer();
        var store = new InMemoryRetryMessageStore();
        // 预置 key=b 的退避中记录: onStart 将 b 加入 blockedKeys，且不会立即被重试排空
        store.save("k11", FakeStreamConsumer.record(0, "b"));
        store.records().get(0).setRetryAt(LocalDateTime.now().plusHours(1));

        // handler 在闸门（arming 后）打开前不返回结果，用于精确制造"停机与落库失败叠加"的窗口
        var gating = new AtomicBoolean(false);
        var invoked = new CountDownLatch(1);
        var gate = new CompletableFuture<Void>();
        var handler = new FakeSinkHandler() {

            @Override
            public CompletableFuture<List<SinkResult>> handle(List<ConsumerRecord> records) {
                if (!gating.get()) {
                    return super.handle(records);
                }
                invoked.countDown();
                return gate.thenApply(v -> super.handle(records).join());
            }
        };
        handler.thenReturn(SinkStatus.SUCCESS, SinkStatus.FAILURE); // 预热批次成功, offset 5 投递失败

        var deliverer = new KeyOrderedDeliverer("k11", handler, 100, closedCircuit(), store);
        var worker = new SinkWorker("k11", consumer, 100, deliverer);
        // 预热批次先建立已提交水位 4：新协议下 onBatch 未返回前不提交，
        // 闸门口批次的水位不会推进，停机提交只能钳在缺口之前
        consumer.offer(List.of(FakeStreamConsumer.record(4, "c")));
        worker.start();
        await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 4);

        gating.set(true);
        consumer.offer(List.of(FakeStreamConsumer.record(5, "a"), FakeStreamConsumer.record(6, "b")));

        // 等 worker 走到 handler：此刻 offset 6（key b 被阻塞分流）已落库，
        // offset 5 的投递结果尚未返回，已提交水位停在 4
        assertTrue(invoked.await(2, TimeUnit.SECONDS));

        // 制造"停机 + 存储故障"叠加窗口后放行投递结果：offset 5 落库将被放弃
        store.failSaves.set(true);
        var shutdown = worker.shutdown();
        gate.complete(null);
        shutdown.get(5, TimeUnit.SECONDS);

        // 停机提交必须停在缺口之前（4）：越过 5 提交就意味着 5 既未投递又未落库却被跳过
        assertEquals(4, consumer.lastCommit());
        // 缺口追踪：即便水位已越过缺口，停机提交也会被钳到缺口之前
        assertEquals(4, deliverer.clampCommit(6));
        // 库中只有预置记录与分流落库的 offset 6；offset 5 未落库，等重启重投
        assertEquals(List.of(0L, 6L),
                store.records().stream().map(RetryRecord::getOffset).sorted().toList());
    }

    @Test
    void storeOutageStallsSaveButDoesNotLoseMessage() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var store = new InMemoryRetryMessageStore();
        store.saveFailures.set(2); // 前两次落库抛异常, saveWithRetry 每秒重试
        var worker = new SinkWorker("k10", consumer, 100,
                new KeyOrderedDeliverer("k10", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 存储瞬断约 2s: saveWithRetry 原地重试, 恢复后落库、提交并重试成功;
            // 直接等终态——落库成功到重试排空之间窗口极短(fake 空轮询不阻塞), 中间态断言会抖
            await().atMost(8, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            assertEquals(0, store.saveFailures.get()); // 故障确实发生并被重试消耗
            assertEquals(0, consumer.lastCommit());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
