package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.extern.slf4j.Slf4j;
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

    @Override
    public void onBatch(List<ConsumerRecord> records, SinkContext ctx) {
        for (var record : records) {
            if (!processRecord(record, ctx)) {
                // INTERRUPTED 或停机：中止本批次
                return;
            }
        }
    }

    private boolean processRecord(ConsumerRecord record, SinkContext ctx) {
        while (ctx.isRunning()) {
            if (ctx.circuitBreaker().isOpen()) {
                LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                continue;
            }
            SinkResult result;
            try {
                result = ctx.handler().handle(record).join();
            } catch (Exception e) {
                log.warn("[{}] handler error on offset={}, retry in place: {}",
                        ctx.name(), record.getOffset(), e.toString());
                LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                continue;
            }
            switch (result.getStatus()) {
                case SUCCESS:
                    ctx.advance(record.getOffset());
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
