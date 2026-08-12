package ravenworks.magpie.soak.verify;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static ravenworks.magpie.soak.verify.SequenceTracker.Semantics.RELAXED;
import static ravenworks.magpie.soak.verify.SequenceTracker.Semantics.STRICT;


class SequenceTrackerTest {

    private static final long GAP_TIMEOUT = 1_000;
    private static final long STALE = 60_000;
    private static final int MAX_PENDING = 5;

    private final List<String> events = new ArrayList<>();
    private final SequenceTracker.Listener listener = new SequenceTracker.Listener() {

        @Override
        public void onReceived() {
            SequenceTrackerTest.this.events.add("received");
        }

        @Override
        public void onDuplicate() {
            SequenceTrackerTest.this.events.add("duplicate");
        }

        @Override
        public void onOutOfOrder() {
            SequenceTrackerTest.this.events.add("outOfOrder");
        }

        @Override
        public void onLost(long count) {
            SequenceTrackerTest.this.events.add("lost:" + count);
        }
    };

    private SequenceTracker tracker(SequenceTracker.Semantics semantics) {
        return new SequenceTracker(semantics, GAP_TIMEOUT, MAX_PENDING, STALE, this.listener);
    }

    @Test
    void strictContiguousDelivery() {
        var tracker = tracker(STRICT);
        for (long seq = 1; seq <= 5; seq++) {
            tracker.onProbe("k1", seq, 0);
        }
        var s = tracker.snapshot();
        assertEquals(5, s.received());
        assertEquals(0, s.duplicates());
        assertEquals(0, s.outOfOrder());
        assertEquals(0, s.lost());
    }

    @Test
    void strictDuplicateIsAllowed() {
        // at-least-once 重投/重放：旧 seq 重复到达不判违规
        var tracker = tracker(STRICT);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 2, 0);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 2, 0);
        tracker.onProbe("k1", 3, 0);
        var s = tracker.snapshot();
        assertEquals(5, s.received());
        assertEquals(2, s.duplicates());
        assertEquals(0, s.outOfOrder());
    }

    @Test
    void strictGapIsOutOfOrderAndResyncs() {
        // 首次见到 seq 4 时 3 从未到达：有人越序；重同步后 5 不再级联误报
        var tracker = tracker(STRICT);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 2, 0);
        tracker.onProbe("k1", 4, 0);
        tracker.onProbe("k1", 5, 0);
        var s = tracker.snapshot();
        assertEquals(1, s.outOfOrder());
        assertEquals(0, s.lost());
    }

    @Test
    void firstSeenSeqEstablishesBaseline() {
        // verifier 晚于 loadgen 启动：首个到达的 seq 之前的缺口不误报
        var tracker = tracker(STRICT);
        tracker.onProbe("k1", 100, 0);
        tracker.onProbe("k1", 101, 0);
        var s = tracker.snapshot();
        assertEquals(0, s.outOfOrder());
        assertEquals(0, s.lost());
    }

    @Test
    void relaxedBuffersReorderAndFills() {
        var tracker = tracker(RELAXED);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 3, 0);
        tracker.onProbe("k1", 4, 0);
        tracker.onProbe("k1", 2, 0); // 乱序到达但在宽限内补齐
        var s = tracker.snapshot();
        assertEquals(4, s.received());
        assertEquals(0, s.duplicates());
        assertEquals(0, s.outOfOrder());
        assertEquals(0, s.lost());
    }

    @Test
    void relaxedDuplicateInPending() {
        var tracker = tracker(RELAXED);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 3, 0);
        tracker.onProbe("k1", 3, 0);
        assertEquals(1, tracker.snapshot().duplicates());
    }

    @Test
    void relaxedGapTimeoutCountsLost() {
        var tracker = tracker(RELAXED);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 4, 0); // 缺 2、3
        tracker.sweep(GAP_TIMEOUT + 1);
        var s = tracker.snapshot();
        assertEquals(2, s.lost());
        // 缺口结算后水位重同步，后续连续 seq 正常推进
        tracker.onProbe("k1", 5, GAP_TIMEOUT + 2);
        assertEquals(0, tracker.snapshot().outOfOrder());
    }

    @Test
    void relaxedPendingOverflowForcesGapClose() {
        var tracker = tracker(RELAXED);
        tracker.onProbe("k1", 1, 0);
        for (long seq = 3; seq <= 3 + MAX_PENDING; seq++) {
            tracker.onProbe("k1", seq, 0);
        }
        // seq 2 在缓冲超限时被判丢失
        assertEquals(1, tracker.snapshot().lost());
    }

    @Test
    void staleKeysAreEvicted() {
        var tracker = tracker(STRICT);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k2", 1, STALE);
        tracker.sweep(STALE + 1);
        assertEquals(1, tracker.snapshot().activeKeys());
    }

    @Test
    void keysAreIndependent() {
        var tracker = tracker(STRICT);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k2", 1, 0);
        tracker.onProbe("k2", 2, 0);
        tracker.onProbe("k1", 2, 0);
        assertEquals(0, tracker.snapshot().outOfOrder());
        assertEquals(2, tracker.snapshot().activeKeys());
    }

    @Test
    void listenerReceivesVerdicts() {
        var tracker = tracker(RELAXED);
        tracker.onProbe("k1", 1, 0);
        tracker.onProbe("k1", 3, 0);
        tracker.onProbe("k1", 3, 0);
        tracker.sweep(GAP_TIMEOUT + 1);
        assertEquals(List.of("received", "received", "received", "duplicate", "lost:1"), this.events);
    }

}
