package ravenworks.magpie.engine.impl.sink.worker;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * Sink 投递 worker：工作循环骨架（拉取一批 → 交给 Deliverer 处置 → 提交水位）。
 * 与 DeliveryMode 相关的处置行为（含熔断、RetryStore）全部归 Deliverer；
 * 本类看守两条安全不变式：水位只前进不后退（前进才提交）、
 * Deliverer 返回部分水位（中断信号）后不再拉取。
 *
 * @author Raven
 */
@Slf4j
public class SinkWorker implements Lifecycle {

    private static final Object POLL_SIGNAL = new Object();
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(50);

    private final String name;
    private final StreamConsumer consumer;
    private final int batchSize;
    private final Deliverer deliverer;
    private final WorkLoop workLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);

    private volatile Thread loopThread;
    /**
     * 部分处置（中断信号）后置位：不再拉取，未处置后缀等重启从未提交 offset 重投
     */
    private boolean halted;

    public SinkWorker(@NonNull String name,
                      @NonNull StreamConsumer consumer,
                      int batchSize,
                      @NonNull Deliverer deliverer) {
        this.name = name;
        this.consumer = consumer;
        this.batchSize = batchSize;
        this.deliverer = deliverer;
        this.workLoop = new WorkLoop("snk-" + name, 1_000, this::dispatch);
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        // 先通知 deliverer，长循环（原地重试、落库重试）尽快退出
        this.deliverer.onShutdown();
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.workLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        if (event instanceof WorkLoopSignal signal) {
            switch (signal) {
                case STARTED -> onStart();
                case IDLE -> pollAndProcess();
                case PRE_SHUTDOWN -> onPreShutdown();
                case TERMINATED -> {
                }
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.consumer.start();
        this.deliverer.onStart();
        this.workLoop.enqueue(POLL_SIGNAL);
    }

    private void onPreShutdown() {
        long committable = this.deliverer.clampCommit(this.lastOffset.get());
        if (committable >= 0) {
            log.info("[{}] committing offset {} before shutdown", this.name, committable);
            this.consumer.commit(committable);
        }
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
        if (!isRunning() || this.halted) {
            return;
        }
        var action = this.deliverer.nextAction();
        if (action == Deliverer.Action.WAIT) {
            // 熔断开启：不拉取、不入队，靠 WorkLoop idle（1s）驱动下一轮
            return;
        }
        try {
            if (action == Deliverer.Action.RETRY) {
                this.deliverer.retryCycle();
            } else {
                pollOnce();
            }
        } catch (Exception e) {
            // 系统性故障（如 Stream/DB 瞬断）：停顿后下轮继续，避免忙转、不中断轮询循环
            log.warn("[{}] poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            this.workLoop.enqueue(POLL_SIGNAL);
        }
    }

    private void pollOnce() {
        var batch = this.consumer.poll(this.batchSize, POLL_TIMEOUT);
        batch = filterByOffset(batch);
        if (batch.isEmpty()) {
            this.deliverer.onEmptyPoll();
            return;
        }
        long watermark = this.deliverer.onBatch(batch);
        long last = this.lastOffset.get();
        if (watermark > last) {
            this.lastOffset.set(watermark);
            this.consumer.commit(watermark);
        }
        if (watermark < batch.getLast().getOffset()) {
            // 部分处置 = 中断信号：后缀未处置且本会话不会再出现，不得再拉取，
            // 等重启后从未提交 offset 重投
            log.warn("[{}] batch partially processed up to offset {} (batch ends at {}), halted",
                    this.name, watermark, batch.getLast().getOffset());
            this.halted = true;
        }
    }

    private List<ConsumerRecord> filterByOffset(List<ConsumerRecord> batch) {
        long last = this.lastOffset.get();
        if (last < 0) {
            return batch;
        }
        int before = batch.size();
        var filtered = batch.stream()
                .filter(r -> r.getOffset() > last)
                .toList();
        int skipped = before - filtered.size();
        if (skipped > 0) {
            log.warn("[{}] skipped {} already-processed messages (offset <= {})",
                    this.name, skipped, last);
        }
        return filtered;
    }

}
