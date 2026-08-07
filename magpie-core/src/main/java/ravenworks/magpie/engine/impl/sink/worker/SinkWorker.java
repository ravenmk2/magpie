package ravenworks.magpie.engine.impl.sink.worker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.Lifecycle;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.impl.sink.deliverer.Deliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.SinkContext;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * Sink 投递 worker：工作循环骨架（拉取一批 → 交给 Deliverer 处置 → 提交水位）。
 * 与 DeliveryMode 相关的处置行为全部委托给 {@link Deliverer}；
 * 本类看守两条 at-least-once 不变式：先落库再推进水位、停机提交不越过未落库缺口。
 *
 * @author Raven
 */
@Slf4j
public class SinkWorker implements Lifecycle, SinkContext {

    private static final Object POLL_SIGNAL = new Object();
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(50);

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final RetryMessageStore retryStore;
    private final int batchSize;
    private final Deliverer deliverer;
    private final WorkLoop workLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);

    private volatile Thread loopThread;
    /** 停机时落库失败的最小 offset：停机提交不得越过它（at-least-once 缺口防护） */
    private long firstUnpersistedOffset = Long.MAX_VALUE;

    public SinkWorker(@NonNull String name,
                      @NonNull StreamConsumer consumer,
                      @NonNull SinkHandler handler,
                      @NonNull CircuitBreaker circuitBreaker,
                      RetryMessageStore retryStore,
                      int batchSize,
                      @NonNull Deliverer deliverer) {
        this.name = name;
        this.consumer = consumer;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
        this.retryStore = retryStore;
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
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.workLoop.shutdown();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public int batchSize() {
        return this.batchSize;
    }

    @Override
    public SinkHandler handler() {
        return this.handler;
    }

    @Override
    public CircuitBreaker circuitBreaker() {
        return this.circuitBreaker;
    }

    @Override
    public RetryMessageStore retryStore() {
        return this.retryStore;
    }

    @Override
    public boolean isRunning() {
        return this.workLoop.getState() == WorkLoopState.RUNNING;
    }

    @Override
    public void advance(long offset) {
        this.lastOffset.updateAndGet(o -> Math.max(o, offset));
    }

    /**
     * 落库包装：停机中落库失败时（saveWithRetry 按设计放弃重试并抛出），先记录未能持久化的
     * 最小 offset 再抛出——onPreShutdown 的提交不得越过它，否则该消息既未投递又未落库，
     * 却随水位提交被跳过，永久丢失。正常运行期 saveWithRetry 原地重试不抛出，不影响正常语义。
     */
    @Override
    public void persist(ConsumerRecord record) {
        try {
            saveWithRetry(record);
        } catch (RuntimeException e) {
            this.firstUnpersistedOffset = Math.min(this.firstUnpersistedOffset, record.getOffset());
            throw e;
        }
    }

    /**
     * 落库失败原地重试：未落库的消息不能推进 offset，否则提交后重启也无法找回。
     * 关闭中不再重试，直接抛出（offset 未提交，等重启重投）。
     */
    private void saveWithRetry(ConsumerRecord record) {
        while (true) {
            try {
                this.retryStore.save(this.name, record);
                return;
            } catch (RuntimeException e) {
                if (!isRunning()) {
                    throw e;
                }
                log.warn("[{}] save retry message failed (offset={}), retry in 1s: {}",
                        this.name, record.getOffset(), e.toString());
                LockSupport.parkNanos(1_000_000_000L);
            }
        }
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        switch (event) {
            case WorkLoop.Started _ -> onStart();
            case WorkLoop.Idle _ -> pollAndProcess();
            case WorkLoop.PreShutdown _ -> onPreShutdown();
            default -> {
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.consumer.start();
        this.deliverer.onStart(this);
        this.workLoop.enqueue(POLL_SIGNAL);
    }

    private void onPreShutdown() {
        // 提交水位不得越过停机时未能落库的消息：只提交缺口之前，其余等重启重投
        long committable = Math.min(this.lastOffset.get(), this.firstUnpersistedOffset - 1);
        if (this.firstUnpersistedOffset <= this.lastOffset.get()) {
            log.warn("[{}] clamping shutdown commit to {}: message at offset {} was not persisted",
                    this.name, committable, this.firstUnpersistedOffset);
        }
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

    private void pollAndProcess() {
        if (!isRunning()) {
            return;
        }
        if (this.circuitBreaker.isOpen()) {
            return;
        }
        try {
            if (this.deliverer.retryPending()) {
                this.deliverer.retryCycle(this);
            } else {
                var batch = this.consumer.poll(this.batchSize, POLL_TIMEOUT);
                batch = filterByOffset(batch);
                if (!batch.isEmpty()) {
                    this.deliverer.onBatch(batch, this);
                    long offset = this.lastOffset.get();
                    if (offset >= 0) {
                        this.consumer.commit(offset);
                    }
                } else {
                    this.deliverer.onEmptyPoll(this);
                }
            }
        } catch (Exception e) {
            // 系统性故障（如 Stream/DB 瞬断）：停顿后下轮继续，避免忙转、不中断轮询循环
            log.warn("[{}] poll/process failed, retry in 1s", this.name, e);
            LockSupport.parkNanos(1_000_000_000L);
        } finally {
            this.workLoop.enqueue(POLL_SIGNAL);
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
