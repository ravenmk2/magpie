package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.concurrent.locks.LockSupport;


/**
 * ORDERED：分区内严格有序，逐条投递，只有 SUCCESS 才前进；
 * FAILURE 与 handler 异常原地重试（节奏与熔断等待一致，连续失败由熔断器接管进一步限速），
 * 失败消息不跳过、不落库、不提交 offset，重启后从未提交处重新投递。
 *
 * @author Raven
 */
@Slf4j
public class OrderedDeliverer implements Deliverer {

    /** 原地重试与熔断等待的停顿节奏 */
    private static final long RETRY_PAUSE_NANOS = 200_000_000L;

    private final String name;
    private final SinkHandler handler;
    private final CircuitBreaker circuitBreaker;
    /** 停机标志：由 onShutdown 从停机线程写入，原地重试循环据此退出 */
    private volatile boolean shutdownRequested;

    public OrderedDeliverer(@NonNull String name,
                            @NonNull SinkHandler handler,
                            @NonNull CircuitBreaker circuitBreaker) {
        this.name = name;
        this.handler = handler;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void onStart() {
        this.shutdownRequested = false;
    }

    @Override
    public void onShutdown() {
        this.shutdownRequested = true;
    }

    @Override
    public Action nextAction() {
        return this.circuitBreaker.isOpen() ? Action.WAIT : Action.POLL;
    }

    @Override
    public long onBatch(List<ConsumerRecord> records) {
        long watermark = -1;
        for (var record : records) {
            if (!processRecord(record)) {
                // INTERRUPTED 或停机：中止本批次，返回已处置前缀（中断信号）
                return watermark;
            }
            watermark = record.getOffset();
        }
        return watermark;
    }

    private boolean processRecord(ConsumerRecord record) {
        while (!this.shutdownRequested) {
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
