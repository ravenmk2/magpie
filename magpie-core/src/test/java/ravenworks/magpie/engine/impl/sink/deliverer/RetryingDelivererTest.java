package ravenworks.magpie.engine.impl.sink.deliverer;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * 退避公式：5s × 2^(attempts-1)，封顶 5min，非法 attempts 不溢出。
 */
class RetryingDelivererTest {

    private static BestEffortDeliverer deliverer() {
        return new BestEffortDeliverer("t", new FakeSinkHandler(), 100,
                new CircuitBreaker("t", 100, 1, 1_000), new InMemoryRetryMessageStore());
    }

    @Test
    void firstRetryDelayIsBaseDelay() {
        assertEquals(5_000, deliverer().retryDelayMillis(1));
    }

    @Test
    void retryDelayDoublesPerAttempt() {
        assertEquals(10_000, deliverer().retryDelayMillis(2));
        assertEquals(20_000, deliverer().retryDelayMillis(3));
        assertEquals(40_000, deliverer().retryDelayMillis(4));
    }

    @Test
    void retryDelayIsCappedAtMax() {
        assertEquals(300_000, deliverer().retryDelayMillis(7));
        assertEquals(300_000, deliverer().retryDelayMillis(100));
        assertEquals(300_000, deliverer().retryDelayMillis(Integer.MAX_VALUE));
    }

    @Test
    void retryDelayNeverOverflows() {
        assertTrue(deliverer().retryDelayMillis(0) > 0);
        assertTrue(deliverer().retryDelayMillis(-1) > 0);
    }

    @Test
    void unknownResultOffsetIsSilentlySkipped() {
        var store = new InMemoryRetryMessageStore();
        store.save("t", FakeStreamConsumer.record(0, "a"));
        var skipped = new AtomicBoolean(false);
        var handler = new FakeSinkHandler() {

            @Override
            public CompletableFuture<List<SinkResult>> handle(List<ConsumerRecord> records) {
                if (skipped.compareAndSet(false, true)) {
                    // 返回不在本批条目表（entryByOffset）中的 offset：命中 continue 分支被跳过
                    return CompletableFuture.completedFuture(List.of(new SinkResult()
                            .setStatus(SinkStatus.SUCCESS)
                            .setAttempts(1)
                            .setRecord(FakeStreamConsumer.record(999, "ghost"))));
                }
                return super.handle(records);
            }
        };
        var deliverer = new BestEffortDeliverer("t", handler, 100,
                new CircuitBreaker("t", 100, 1, 1_000), store);
        deliverer.init();

        deliverer.retry();

        // 首轮幽灵结果被跳过（条目未被误标成功或失败）；下一轮正常重投成功并排空
        assertEquals(0, store.size());
        assertEquals(1, handler.countByOffset(0));
    }

    @Test
    void failedGroupAbortsBeforeNextGroupIsSent() {
        var store = new InMemoryRetryMessageStore();
        store.save("t", FakeStreamConsumer.record(0, "a"));
        store.save("t", FakeStreamConsumer.record(1, "a"));
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        // KEY_ORDERED 按 key 切组：同 key 两条记录分成两组 [0]、[1]
        var deliverer = new KeyOrderedDeliverer("t", handler, 100,
                new CircuitBreaker("t", 100, 1, 1_000), store);
        deliverer.init();

        deliverer.retry();

        // 组 [0] 失败即 break 中断本轮排空：组 [1] 在本轮从未发给 handler
        assertEquals(1, handler.handledBatches.size());
        assertEquals(0, handler.countByOffset(1));
        // 失败条目退避落库（attempts+1），未轮到的条目原样保留
        assertEquals(2, store.size());
        assertEquals(1, store.records().get(0).getAttempts());
        assertEquals(0, store.records().get(1).getAttempts());
    }

    @Test
    void retryDelayAlignsToEarliestFailedRetryAt() {
        var store = new InMemoryRetryMessageStore();
        store.save("t", FakeStreamConsumer.record(0, "a"));
        store.save("t", FakeStreamConsumer.record(1, "b"));
        // 两条条目 attempts 不同 → 失败后退避时长不同：offset 0 短退避先到期
        store.records().get(1).setAttempts(4);
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE, SinkStatus.FAILURE);
        var deliverer = new BestEffortDeliverer("t", handler, 100,
                new CircuitBreaker("t", 100, 1, 1_000), store) {

            @Override
            protected long retryDelayMillis(int attempts) {
                // 短退避 200ms（第 1 次失败）/ 长退避 60s（第 5 次失败）
                return attempts <= 1 ? 200 : 60_000;
            }
        };
        deliverer.init();

        deliverer.retry();

        // 两个失败项都按各自退避落库
        assertEquals(2, store.size());
        // 重试时点按最早失败项（≈200ms）对齐：若误取最晚项（60s），下面的 await 会超时
        assertFalse(deliverer.canRetry());
        await().atMost(2, TimeUnit.SECONDS).until(deliverer::canRetry);
    }

}
