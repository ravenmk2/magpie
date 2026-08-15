package ravenworks.magpie.engine.impl.sink;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.Lifecycle;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.impl.sink.deliverer.Deliverer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;


/**
 * Sink 投递 worker：工作循环骨架（拉取一批 → 交给 Deliverer 处置 → 按节流提交水位）。
 * 与 DeliveryMode 相关的处置行为（含熔断、RetryStore、重试排空）全部归 Deliverer；
 * 本类看守两条安全不变式：committableOffset 只随 Deliverer 返回的水位前进
 * （Deliverer 投递成功或落库后才返回水位，提交因此天然不越过未处置缺口）、
 * Deliverer 返回 completed=false（中断信号）后立即提交已处置前缀并不再拉取。
 *
 * <p>三个 offset 分工：lastOffset 随 poll 推进，会话内按它过滤重放消息；
 * committableOffset 是已处置水位，为提交的唯一来源；committedOffset 记录已提交水位。
 * 提交按 commitInterval 节流（避免每批一次存储 IO），中断与停机时立即提交。
 *
 * <p>启动失败（onStart 中外部依赖不可用）：worker 标记为死亡（isAlive=false），
 * 不再拉取，由上层 reconcile 观测后退役重建——重建即启动重试。
 *
 * @author Raven
 */
@Slf4j
public class SinkWorker implements Lifecycle {

    private static final Object POLL_SIGNAL = new Object();
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(50);
    /**
     * Deliverer 未就绪（熔断开启）时的停顿节奏
     */
    private static final long NOT_READY_PAUSE_NANOS = 200_000_000L;

    private final String name;
    private final StreamConsumer consumer;
    private final int batchSize;
    private final long commitInterval;
    private final Deliverer deliverer;
    private final WorkLoop workLoop;

    // 以下字段全部 confined 在 WorkLoop 线程（dispatch 回调内）访问，无需同步
    private long lastOffset = -1;
    private long committableOffset = -1;
    private long committedOffset = -1;
    private long lastCommitAt;

    private volatile Thread loopThread;
    /**
     * onStart 失败（外部依赖暂不可用）后置位：上报死亡，
     * 由上层 reconcile 的 isAlive 观测退役重建
     */
    private volatile boolean startFailed;
    /**
     * 部分处置（中断信号）后置位：不再拉取，未处置后缀等重启从未提交 offset 重投
     */
    private boolean halted;

    public SinkWorker(@NonNull String name,
                      @NonNull StreamConsumer consumer,
                      int batchSize,
                      long commitInterval,
                      @NonNull Deliverer deliverer) {
        this.name = name;
        this.consumer = consumer;
        this.batchSize = batchSize;
        this.commitInterval = commitInterval;
        this.deliverer = deliverer;
        this.workLoop = new WorkLoop("snk-" + name, 1_000, this::dispatch);
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        // 先通知 deliverer，长循环（原地重试、落库重试、重试排空）尽快退出
        this.deliverer.interrupt();
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        // 循环线程异常死亡（Error）时 PRE_SHUTDOWN 不会执行，须兜底停掉 consumer，
        // 避免底层订阅挂在死线程上泄漏；死亡异常已由 WorkLoop 记录，这里吞掉，
        // 使"停止一个已死 worker"成为干净的成功路径
        return this.workLoop.shutdown().handle((v, e) -> {
            this.stopConsumer();
            return null;
        });
    }

    @Override
    public boolean isAlive() {
        return !this.startFailed && this.workLoop.isAlive();
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        if (event instanceof WorkLoopSignal signal) {
            switch (signal) {
                case STARTED -> this.onStart();
                case IDLE -> this.pollAndProcess();
                case PRE_SHUTDOWN -> this.onPreShutdown();
                case TERMINATED -> {
                }
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.lastCommitAt = System.currentTimeMillis();
        try {
            this.consumer.start();
            this.deliverer.init();
        } catch (Exception e) {
            // 外部依赖暂不可用：标记死亡并回收可能已启动的 consumer，
            // reconcile 观测到 isAlive=false 后退役重建，重建即重试
            this.startFailed = true;
            this.stopConsumer();
            log.error("[{}] failed to start, waiting for reconcile to restart", this.name, e);
            return;
        }
        this.workLoop.enqueue(POLL_SIGNAL);
    }

    private void onPreShutdown() {
        this.commitOffset();
        this.stopConsumer();
    }

    /**
     * 幂等停止底层 consumer：正常停机由 PRE_SHUTDOWN 在循环线程内调用，
     * 循环异常死亡时由 shutdown() 兜底调用。
     */
    private void stopConsumer() {
        try {
            this.consumer.stop();
        } catch (Exception ex) {
            log.warn("[{}] error stopping consumer", this.name, ex);
        }
    }

    private boolean isRunning() {
        return this.workLoop.getState() == WorkLoopState.RUNNING;
    }

    private void pollAndProcess() {
        if (!this.isRunning() || this.startFailed) {
            // 启动失败：等 reconcile 重建，不触碰未初始化的 consumer/deliverer
            return;
        }
        this.maybeCommitByTime();
        if (this.halted) {
            return;
        }
        try {
            if (!this.deliverer.isReady()) {
                // 熔断开启：不拉取，停顿后下轮再探
                LockSupport.parkNanos(NOT_READY_PAUSE_NANOS);
            } else {
                this.pollOnce();
            }
        } catch (Exception e) {
            // 系统性故障（如 Stream/DB 瞬断）：停顿后下轮继续，避免忙转、不中断轮询循环
            log.warn("[{}] poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            // 停机后不再自续轮询，避免 enqueue 撞 SHUTTING_DOWN 打丢弃告警
            if (this.isRunning()) {
                this.workLoop.enqueue(POLL_SIGNAL);
            }
        }
    }

    private void pollOnce() {
        var batch = this.consumer.poll(this.batchSize, POLL_TIMEOUT);
        batch = this.filterByLastOffset(batch);
        if (batch.isEmpty()) {
            // 空闲窗口：交给 Deliverer 决定是否排空重试
            if (this.deliverer.canRetry()) {
                this.deliverer.retry();
            }
            return;
        }
        // lastOffset 随 poll 推进而非随提交：会话内去重只看是否拉取过
        this.lastOffset = batch.getLast().getOffset();
        var outcome = this.deliverer.deliver(batch);
        if (outcome.watermark() > this.committableOffset) {
            this.committableOffset = outcome.watermark();
        }
        if (!outcome.completed()) {
            // 中断信号：立即提交已处置前缀，不再拉取，
            // 未处置后缀等重启后从未提交 offset 重投
            log.warn("[{}] batch partially processed up to offset {} (batch ends at {}), halted",
                    this.name, outcome.watermark(), batch.getLast().getOffset());
            this.halted = true;
            this.commitOffset();
        }
    }

    /**
     * 距上次提交超过 commitInterval 且有未提交进展时才提交，避免每批一次存储 IO。
     */
    private void maybeCommitByTime() {
        if (this.committableOffset > this.committedOffset
                && System.currentTimeMillis() - this.lastCommitAt >= this.commitInterval) {
            this.commitOffset();
        }
    }

    /**
     * 提交已处置水位（无进展则空转）。提交来源只有 committableOffset——
     * 它只随 Deliverer 返回的水位前进，天然不越过未投递/未落库的缺口。
     * 提交失败（如 Stream/DB 瞬断）只记日志并跳过本次：内存态不回退，
     * 下个周期带着相同或更大的水位自然重试；停机路径同理，不阻断停机。
     */
    private void commitOffset() {
        long watermark = this.committableOffset;
        if (watermark <= this.committedOffset) {
            return;
        }
        log.debug("[{}] committing offset {}", this.name, watermark);
        try {
            this.consumer.commit(watermark);
        } catch (Exception e) {
            log.error("[{}] failed to commit offset {}, skipped", this.name, watermark, e);
            return;
        }
        this.committedOffset = watermark;
        this.lastCommitAt = System.currentTimeMillis();
    }

    private List<ConsumerRecord> filterByLastOffset(List<ConsumerRecord> batch) {
        // 批次按 offset 有序，首条合格即全部合格：直接返回原批次，
        // 避免每次 poll 都为几乎不会发生的过滤分配 stream 与新列表
        if (batch.isEmpty() || this.lastOffset < 0 ||
                batch.getFirst().getOffset() > this.lastOffset) {
            return batch;
        }
        int before = batch.size();
        var filtered = batch.stream()
                .filter(r -> r.getOffset() > this.lastOffset)
                .toList();
        int skipped = before - filtered.size();
        if (skipped > 0) {
            log.warn("[{}] skipped {} already-processed messages (offset <= {})",
                    this.name, skipped, this.lastOffset);
        }
        return filtered;
    }

}
