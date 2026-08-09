package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.NonNull;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;


/**
 * BEST_EFFORT：不保证顺序，整批投递追求吞吐；失败消息先落 RetryStore 再推进水位，
 * 最少一次保证与 KEY_ORDERED 一致。存量重试消息（含重启后遗留）在首个空闲窗口排空。
 *
 * @author Raven
 */
public class BestEffortDeliverer extends RetryingDeliverer {

    public BestEffortDeliverer(@NonNull String name,
                               @NonNull SinkHandler handler,
                               int batchSize,
                               @NonNull CircuitBreaker circuitBreaker,
                               @NonNull RetryMessageStore retryStore) {
        super(name, handler, batchSize, circuitBreaker, retryStore);
    }

    @Override
    public BatchOutcome deliver(List<ConsumerRecord> records) {
        long watermark = -1;
        List<SinkResult> results = this.handler.handle(records).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                this.persist(result.getRecord());
            }
            watermark = Math.max(watermark, result.getRecord().getOffset());
        }
        return new BatchOutcome(watermark, true);
    }

    @Override
    protected List<RetryRecord> fetchRetryEntries() {
        // 只取到期项：无顺序要求，直接依赖 retryAt 字段（Deliverer 计算落库，重启后仍生效）
        return this.retryStore.listRetryable(this.name, this.batchSize);
    }

    @Override
    protected void onFetchEmpty() {
        // 没有到期项不代表存储为空：退避中的消息仍在，hasRetryable 不能复位
        this.hasRetryable = !this.retryStore.list(this.name, 1).isEmpty();
        if (!this.hasRetryable) {
            this.onRetryDrained();
        } else {
            // 全部退避中：推后重试时点，到期前不再查库
            this.postponeRetry();
        }
    }

    @Override
    protected List<List<ConsumerRecord>> group(List<ConsumerRecord> records) {
        return List.of(records);
    }

}
