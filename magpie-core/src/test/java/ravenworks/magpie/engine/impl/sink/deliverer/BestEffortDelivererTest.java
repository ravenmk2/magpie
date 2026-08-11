package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.impl.sink.SinkWorker;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class BestEffortDelivererTest {

    // 测试用短提交间隔：节流提交在 await 窗口内尽快生效
    private static final long COMMIT_INTERVAL = 50;

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    /**
     * 测试用 deliverer：退避缩短到 50ms，避免真实 5s 退避拖垮断言窗口
     */
    private static BestEffortDeliverer deliverer(String name, FakeSinkHandler handler,
                                                 InMemoryRetryMessageStore store) {
        return new BestEffortDeliverer(name, handler, 100, closedCircuit(), store) {

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
        var worker = new SinkWorker("b1", consumer, 100, COMMIT_INTERVAL,
                new BestEffortDeliverer("b1", handler, 100, closedCircuit(), store));
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
        var worker = new SinkWorker("b2", consumer, 100, COMMIT_INTERVAL,
                new BestEffortDeliverer("b2", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            // offset 1 第一次失败落库，重试成功后再发送一次；计数单调递增，不受瞬态影响
            await().atMost(3, TimeUnit.SECONDS).until(() -> handler.countByOffset(1) == 2);
            assertEquals(0, store.size());
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
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
        var worker = new SinkWorker("b3", consumer, 100, COMMIT_INTERVAL,
                deliverer("b3", handler, store));
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
        var worker = new SinkWorker("b4", consumer, 100, COMMIT_INTERVAL,
                new BestEffortDeliverer("b4", handler, 100, closedCircuit(), store));
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            // 存储瞬断约 1s: saveWithRetry 原地重试, 恢复后落库、提交并重试成功;
            // 直接等终态——落库成功到重试排空之间窗口极短(fake 空轮询不阻塞), 中间态断言会抖
            await().atMost(8, TimeUnit.SECONDS).until(() -> store.size() == 0 && handler.countByOffset(0) == 2);
            assertEquals(0, store.saveFailures.get()); // 故障确实发生并被重试消耗
            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 0);
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    /**
     * 统计查询次数的存储：验证 canRetry 纯内存判断、不打存储
     */
    private static class CountingStore extends InMemoryRetryMessageStore {

        final AtomicInteger queries = new AtomicInteger();

        @Override
        public List<RetryRecord> list(String consumer, int count) {
            this.queries.incrementAndGet();
            return super.list(consumer, count);
        }

        @Override
        public List<RetryRecord> listRetryable(String consumer, int count) {
            this.queries.incrementAndGet();
            return super.listRetryable(consumer, count);
        }

    }

    @Test
    void backoffOnlyEntriesPostponeRetryWithoutDraining() {
        var handler = new FakeSinkHandler();
        var store = new InMemoryRetryMessageStore();
        store.save("b5", FakeStreamConsumer.record(0, "a"));
        // 全部条目退避中：retryAt 推到未来，listRetryable 取不到到期项
        store.failed(store.records().get(0).getId(), LocalDateTime.now().plusSeconds(60));

        var deliverer = new BestEffortDeliverer("b5", handler, 100, closedCircuit(), store);
        deliverer.init();
        assertTrue(deliverer.canRetry()); // 库非空且内存时点已到：应发起一轮排空探测

        deliverer.retry();

        // 没有到期项不等于排空（区别于 drained）：条目仍在库中、未投递，
        // hasRetryable 保持 true，重试时点按探活节拍推后，到期前 canRetry 为 false
        assertEquals(1, store.size());
        assertTrue(handler.handledRecords.isEmpty());
        assertFalse(deliverer.canRetry());
    }

    @Test
    void successfulDrainResetsHasRetryableAndCanRetryStaysInMemory() {
        var handler = new FakeSinkHandler();
        var store = new CountingStore();
        store.save("b6", FakeStreamConsumer.record(0, "a"));

        var deliverer = new BestEffortDeliverer("b6", handler, 100, closedCircuit(), store);
        deliverer.init();
        assertTrue(deliverer.canRetry());

        deliverer.retry(); // 到期项重投成功（默认 SUCCESS），库真正排空（drained）

        assertEquals(0, store.size());
        // 排空后 hasRetryable 复位：canRetry 短路返回 false，空转期每次空轮询都不再查库
        store.queries.set(0);
        assertFalse(deliverer.canRetry());
        assertFalse(deliverer.canRetry());
        assertEquals(0, store.queries.get());
    }

}
