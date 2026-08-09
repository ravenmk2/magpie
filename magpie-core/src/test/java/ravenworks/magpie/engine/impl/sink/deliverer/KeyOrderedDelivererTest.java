package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.impl.sink.SinkWorker;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class KeyOrderedDelivererTest {

    // 测试用短提交间隔：节流提交在 await 窗口内尽快生效
    private static final long COMMIT_INTERVAL = 50;

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    /**
     * 测试用 deliverer：退避缩短到 50ms，避免真实 5s 退避拖垮断言窗口
     */
    private static KeyOrderedDeliverer deliverer(String name, FakeSinkHandler handler,
                                                 InMemoryRetryMessageStore store) {
        return new KeyOrderedDeliverer(name, handler, 100, closedCircuit(), store) {

            @Override
            protected long retryDelayMillis(int attempts) {
                return 50;
            }
        };
    }

    @Test
    void successfulBatchCommitsOffset() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        var worker = new SinkWorker("k1", consumer, 100, COMMIT_INTERVAL,
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
        var worker = new SinkWorker("k2", consumer, 100, COMMIT_INTERVAL,
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
        var worker = new SinkWorker("k3", consumer, 100, COMMIT_INTERVAL,
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
        var worker = new SinkWorker("k4", consumer, 100, COMMIT_INTERVAL,
                deliverer("k4", handler, store));
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
        var worker = new SinkWorker("k5", consumer, 100, COMMIT_INTERVAL,
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
        var worker = new SinkWorker("k6", consumer, 100, COMMIT_INTERVAL,
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
    void retryBackoffDefersNextAttempt() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE, SinkStatus.FAILURE); // 原始投递失败 + 首次重试失败
        var store = new InMemoryRetryMessageStore();
        var worker = new SinkWorker("k7", consumer, 100, COMMIT_INTERVAL,
                new KeyOrderedDeliverer("k7", handler, 100, closedCircuit(), store) {

                    @Override
                    protected long retryDelayMillis(int attempts) {
                        return 400; // 退避由 Deliverer 内存时点执行，测试用短退避
                    }
                });
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 原始失败 + 首次重试失败迅速发生
            await().atMost(2, TimeUnit.SECONDS).until(() -> handler.countByOffset(0) == 2);
            // 退避期（400ms）内不再重试
            Thread.sleep(200);
            assertEquals(2, handler.countByOffset(0));

            // 退避到期后自动重试成功（默认 SUCCESS）并排空
            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0);
            assertEquals(3, handler.countByOffset(0));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void newerSameKeyEntryWaitsForOlderRetrySuccess() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE); // offset 0 原始投递失败，之后全部成功
        var store = new InMemoryRetryMessageStore();
        var worker = new SinkWorker("k8", consumer, 100, COMMIT_INTERVAL,
                new KeyOrderedDeliverer("k8", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            consumer.offer(List.of(FakeStreamConsumer.record(1, "a"), FakeStreamConsumer.record(2, "b")));
            worker.start();

            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(2) == 1);
            // 同 key 的 offset 1 不越过 offset 0 直发：分流落库后在 offset 0 重试成功之后按序到达；
            // 其他 key（offset 2）不受影响。顺序由"offset 升序读取 + 按序投递 + 失败中断"保证，不依赖 retryAt
            var delivered = handler.handledRecords.stream().map(ConsumerRecord::getOffset).toList();
            assertEquals(List.of(0L, 2L, 0L, 1L), delivered);
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
        var worker = new SinkWorker("k9", consumer, 100, COMMIT_INTERVAL,
                new KeyOrderedDeliverer("k9", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, null), FakeStreamConsumer.record(1, null)));
            worker.start();

            await().atMost(3, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            // offset 0 失败后 key("") 阻塞: offset 1 不越过它直发, 等重试按序到达
            var delivered = handler.handledRecords.stream().map(ConsumerRecord::getOffset).toList();
            assertEquals(List.of(0L, 0L, 1L), delivered);
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void shutdownCommitDoesNotSkipUnpersistedFailure() throws Exception {
        var consumer = new FakeStreamConsumer();
        var store = new InMemoryRetryMessageStore();
        // 预置 key=b 的失败记录: init 将 b 加入 blockedKeys；启动后会被立即重试（KeyOrdered
        // 不读 retryAt），脚本中的 FAILURE 让这次重试失败、记录留在库中
        store.save("k11", FakeStreamConsumer.record(0, "b"));

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
        // 脚本：预热批次（offset 4）成功；预置记录（offset 0）的启动重试失败留在库中；
        // 闸门口批次的 offset 5 投递失败。失败间无固定先后——三个结果按消费顺序生效即可
        handler.thenReturn(SinkStatus.SUCCESS, SinkStatus.FAILURE, SinkStatus.FAILURE);

        var deliverer = new KeyOrderedDeliverer("k11", handler, 100, closedCircuit(), store);
        var worker = new SinkWorker("k11", consumer, 100, COMMIT_INTERVAL, deliverer);
        // 预热批次先建立已提交水位 4：onBatch 未返回前水位不推进，
        // 闸门口批次在停机中放弃落库后中断，committableOffset 停在缺口之前
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
        var worker = new SinkWorker("k10", consumer, 100, COMMIT_INTERVAL,
                new KeyOrderedDeliverer("k10", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 存储瞬断约 2s: saveWithRetry 原地重试, 恢复后落库、提交并重试成功;
            // 直接等终态——落库成功到重试排空之间窗口极短(fake 空轮询不阻塞), 中间态断言会抖
            await().atMost(8, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            assertEquals(0, store.saveFailures.get()); // 故障确实发生并被重试消耗
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 0);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void isReadyIsFalseWhileCircuitBreakerIsOpen() {
        var circuitBreaker = new CircuitBreaker("t", 1, 1, 10_000);
        var deliverer = new KeyOrderedDeliverer("k12", new FakeSinkHandler(), 100,
                circuitBreaker, new InMemoryRetryMessageStore());
        assertTrue(deliverer.isReady());

        // 达到阈值 1，熔断开启：SinkWorker 据此停止拉取
        circuitBreaker.recordFailure();
        assertFalse(deliverer.isReady());
    }

    @Test
    void fullyBlockedBatchDivertsWithoutCallingHandler() {
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        store.save("k13", FakeStreamConsumer.record(0, "a"));
        store.save("k13", FakeStreamConsumer.record(1, "a"));
        var deliverer = new KeyOrderedDeliverer("k13", handler, 100, closedCircuit(), store);
        deliverer.init(); // 从库加载 blockedKeys={a}

        var outcome = deliverer.deliver(List.of(
                FakeStreamConsumer.record(5, "a"),
                FakeStreamConsumer.record(6, "a")));

        // 全部消息命中阻塞 key：不经 handler 直接分流落库，水位纯靠分流推进
        assertTrue(handler.handledRecords.isEmpty());
        assertEquals(6, outcome.watermark());
        assertTrue(outcome.completed());
        assertEquals(4, store.size());
    }

    @Test
    void interruptedResultIsPersistedAsFailure() {
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.INTERRUPTED);
        var store = new InMemoryRetryMessageStore();
        var deliverer = new KeyOrderedDeliverer("k14", handler, 100, closedCircuit(), store);
        deliverer.init();

        var outcome = deliverer.deliver(List.of(FakeStreamConsumer.record(0, "a")));

        // 现状固化：processSubBatch 中 INTERRUPTED 与 FAILURE 同等处置——落库 + 阻塞 key，水位照常推进
        assertEquals(0, outcome.watermark());
        assertTrue(outcome.completed());
        assertEquals(1, store.size());
        assertEquals(Set.of("a"), store.listKeys("k14"));

        // key 已阻塞：同 key 后续消息分流落库，不再投递
        deliverer.deliver(List.of(FakeStreamConsumer.record(1, "a")));
        assertEquals(1, handler.handledRecords.size());
        assertEquals(2, store.size());
    }

}
