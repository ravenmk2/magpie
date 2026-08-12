package ravenworks.magpie.soak.verify;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;


/**
 * 序列校验器：soak 的正确性裁判。按 key 维护探针序号状态机，判定
 * 丢失 / 乱序 / 重复。at-least-once 语义下重复与重放是合法的，不判违规：
 *
 * <ul>
 *   <li>STRICT（ORDERED / KEY_ORDERED 通道）：同 key 首次见到的 seq 必须恰好是
 *       maxSeq+1。有序投递保证先见旧消息才见新消息；首次见到更大的 seq 即有人越序，
 *       判一次 outOfOrder 并把 maxSeq 重同步到该 seq（避免级联误报）。</li>
 *   <li>RELAXED（BEST_EFFORT 通道）：允许乱序到达，未来 seq 进 pending 缓冲；
 *       缺口超过 gapTimeout 未补齐判 lost（按缺口长度计数）；pending 超上限
 *       （maxPendingPerKey，防内存膨胀）时立即按同样规则收口。</li>
 * </ul>
 * <p>
 * 首次见到的 key 以首个到达的 seq 建基线（maxSeq = seq-1），verifier 启动
 * 晚于 loadgen 或自身重启时不会把历史缺口误报为违规。
 */
public class SequenceTracker {

    public enum Semantics {
        STRICT, RELAXED
    }


    /**
     * 判定事件出口：计量/告警由外部实现，便于脱离 Spring/Micrometer 单测
     */
    public interface Listener {

        void onReceived();

        void onDuplicate();

        void onOutOfOrder();

        void onLost(long count);

    }


    private final Semantics semantics;
    private final long gapTimeoutMs;
    private final int maxPendingPerKey;
    private final long staleMs;
    private final Listener listener;
    private final Map<String, KeyState> keys = new HashMap<>();

    private long received;
    private long duplicates;
    private long outOfOrder;
    private long lost;

    public SequenceTracker(Semantics semantics, long gapTimeoutMs, int maxPendingPerKey,
                           long staleMs, Listener listener) {
        this.semantics = semantics;
        this.gapTimeoutMs = gapTimeoutMs;
        this.maxPendingPerKey = maxPendingPerKey;
        this.staleMs = staleMs;
        this.listener = listener;
    }

    public synchronized void onProbe(String key, long seq, long nowMs) {
        this.received++;
        this.listener.onReceived();
        KeyState ks = this.keys.computeIfAbsent(key, k -> KeyState.baseline(seq, nowMs));
        ks.lastSeen = nowMs;
        if (seq <= ks.maxSeq || ks.pending.contains(seq)) {
            this.duplicates++;
            this.listener.onDuplicate();
            return;
        }
        if (seq == ks.maxSeq + 1) {
            advance(ks, seq);
            return;
        }
        if (this.semantics == Semantics.STRICT) {
            this.outOfOrder++;
            this.listener.onOutOfOrder();
            ks.maxSeq = seq;
            return;
        }
        ks.pending.add(seq);
        if (ks.gapSince < 0) {
            ks.gapSince = nowMs;
        }
        if (ks.pending.size() > this.maxPendingPerKey) {
            closeGap(ks, nowMs);
        }
    }

    /**
     * 周期巡检：结算超时缺口、驱逐长期不活跃 key（verifier 状态只增不减会缓慢膨胀）
     */
    public synchronized void sweep(long nowMs) {
        Iterator<Map.Entry<String, KeyState>> it = this.keys.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            KeyState ks = entry.getValue();
            if (!ks.pending.isEmpty() && ks.gapSince >= 0 && nowMs - ks.gapSince > this.gapTimeoutMs) {
                closeGap(ks, nowMs);
            }
            if (nowMs - ks.lastSeen > this.staleMs) {
                it.remove();
            }
        }
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(this.received, this.duplicates, this.outOfOrder, this.lost, this.keys.size());
    }

    private void advance(KeyState ks, long seq) {
        ks.maxSeq = seq;
        while (!ks.pending.isEmpty() && ks.pending.first() == ks.maxSeq + 1) {
            ks.maxSeq = ks.pending.pollFirst();
        }
        if (ks.pending.isEmpty()) {
            ks.gapSince = -1;
        }
    }

    /**
     * 把 pending 中最早 seq 之前的缺口判为丢失，重同步水位后继续排空
     */
    private void closeGap(KeyState ks, long nowMs) {
        long skipped = ks.pending.first() - ks.maxSeq - 1;
        this.lost += skipped;
        this.listener.onLost(skipped);
        advance(ks, ks.pending.first() - 1);
        if (!ks.pending.isEmpty()) {
            ks.gapSince = nowMs;
        }
    }

    public record Snapshot(long received, long duplicates, long outOfOrder, long lost, int activeKeys) {

    }


    private static final class KeyState {

        private long maxSeq;
        private final TreeSet<Long> pending = new TreeSet<>();
        private long gapSince = -1;
        private long lastSeen;

        private static KeyState baseline(long firstSeq, long nowMs) {
            var ks = new KeyState();
            ks.maxSeq = firstSeq - 1;
            ks.lastSeen = nowMs;
            return ks;
        }

    }

}
