package ravenworks.magpie.engine.sink.common;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.runtime.EventLoopState;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkHandler;
import ravenworks.magpie.engine.sink.SinkResult;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class OrderedSinkWorker implements SinkWorker {

    private static final Object POLL_SIGNAL = new Object();
    /** 原地重试与熔断等待的停顿节奏 */
    private static final long RETRY_PAUSE_NANOS = 200_000_000L;

    private final String name;
    private final StreamConsumer consumer;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    private final EventLoop eventLoop;
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final int batchSize;

    private volatile Thread loopThread;

    public OrderedSinkWorker(@NonNull String name,
                             @NonNull StreamConsumer consumer,
                             @NonNull SinkHandler handler,
                             @NonNull CircuitBreaker circuitBreaker,
                             int batchSize) {
        this.name = name;
        this.consumer = consumer;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
        this.batchSize = batchSize;
        this.eventLoop = new EventLoop("snk-" + name, 1_000, this::dispatch);
    }

    @Override
    public void start() {
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        Thread t = this.loopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event == POLL_SIGNAL) {
            this.pollAndProcess();
            return;
        }
        switch (event) {
            case EventLoop.Started _ -> onStart();
            case EventLoop.Idle _ -> onIdle();
            case EventLoop.PreShutdown _ -> onPreShutdown();
            default -> {
            }
        }
    }

    private void onStart() {
        this.loopThread = Thread.currentThread();
        this.consumer.start();
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void onIdle() {
        this.pollAndProcess();
    }

    private void onPreShutdown() {
        long lastOffset = this.lastOffset.get();
        if (lastOffset >= 0) {
            log.info("[{}] committing offset {} before shutdown", this.name, lastOffset);
            this.consumer.commit(lastOffset);
        }
        try {
            this.consumer.stop();
        } catch (Exception ex) {
            log.warn("[{}] error stopping consumer", this.name, ex);
        }
    }

    private void pollAndProcess() {
        if (this.eventLoop.getState() != EventLoopState.RUNNING) {
            return;
        }
        if (this.circuitBreaker.isOpen()) {
            return;
        }
        var batch = this.consumer.poll(this.batchSize, Duration.ofMillis(50));
        batch = SinkWorkerUtils.filterByOffset(this.name, batch, this.lastOffset.get());
        if (!batch.isEmpty()) {
            processBatch(batch);
            long offset = this.lastOffset.get();
            if (offset >= 0) {
                this.consumer.commit(offset);
            }
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private void processBatch(List<ConsumerRecord> batch) {
        for (var record : batch) {
            if (!processRecord(record)) {
                return;
            }
        }
    }

    /**
     * ORDERED 语义：只有 SUCCESS 才前进；FAILURE 与 handler 异常原地重试（节奏与熔断等待一致，
     * 连续失败由熔断器接管进一步限速），INTERRUPTED 或停机时中止本批次。
     * 失败消息不跳过、不落库、不提交 offset，重启后从未提交处重新投递。
     */
    private boolean processRecord(ConsumerRecord record) {
        while (this.eventLoop.getState() == EventLoopState.RUNNING) {
            if (this.circuitBreaker.isOpen()) {
                LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                continue;
            }
            SinkResult result;
            try {
                result = this.handler.handle(record).join();
            } catch (Exception e) {
                log.warn("[{}] handler error on offset={}, retry in place: {}",
                        this.name, record.getOffset(), e.toString());
                LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                continue;
            }
            switch (result.getStatus()) {
                case SUCCESS:
                    this.lastOffset.set(record.getOffset());
                    return true;
                case BACKOFF:
                    break;
                case FAILURE:
                    LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                    break;
                case INTERRUPTED:
                    return false;
            }
        }
        return false;
    }

}
