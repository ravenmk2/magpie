package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * KEY_ORDERED：同一 BusinessKey 按 offset 顺序完成投递，不同 Key 并行互不影响。
 * 失败消息先落 RetryStore 再推进水位；某 Key 失败后进入 blockedKeys，后续同 Key 消息
 * 不再尝试投递，直接分流落库排在已存消息之后；其他 Key 的正常投递不受影响。
 *
 * @author Raven
 */
@Slf4j
public class KeyOrderedDeliverer extends RetryingDeliverer {

    private final Set<String> blockedKeys = new HashSet<>();

    public KeyOrderedDeliverer(@NonNull String name,
                               @NonNull SinkHandler handler,
                               int batchSize,
                               @NonNull CircuitBreaker circuitBreaker,
                               @NonNull RetryMessageStore retryStore) {
        super(name, handler, batchSize, circuitBreaker, retryStore);
    }

    @Override
    public void init() {
        super.init();
        this.blockedKeys.addAll(this.retryStore.listKeys(this.name));
        if (!this.blockedKeys.isEmpty()) {
            log.info("[{}] loaded {} blocked keys from retry store",
                    this.name, this.blockedKeys.size());
        }
    }

    @Override
    public BatchOutcome deliver(List<ConsumerRecord> records) {
        long watermark = -1;

        // 分流：key 已阻塞的消息不再尝试投递，直接落库排在已存消息之后
        List<ConsumerRecord> remaining = records;
        if (!this.blockedKeys.isEmpty()) {
            remaining = new ArrayList<>();
            for (var record : records) {
                if (this.isBlocked(record)) {
                    this.persist(record);
                    watermark = Math.max(watermark, record.getOffset());
                } else {
                    remaining.add(record);
                }
            }
        }

        for (var subBatch : MessageUtils.batchByUniqueKey(remaining, KeyOrderedDeliverer::keyOf)) {
            watermark = Math.max(watermark, this.processSubBatch(subBatch));
        }
        return new BatchOutcome(watermark, true);
    }

    private long processSubBatch(List<ConsumerRecord> subBatch) {
        long watermark = -1;
        List<ConsumerRecord> toSend = new ArrayList<>();
        for (var record : subBatch) {
            if (this.isBlocked(record)) {
                this.persist(record);
                watermark = Math.max(watermark, record.getOffset());
            } else {
                toSend.add(record);
            }
        }
        if (toSend.isEmpty()) {
            return watermark;
        }
        List<SinkResult> results = this.handler.handle(toSend).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                this.persist(result.getRecord());
                this.blockedKeys.add(keyOf(result.getRecord()));
            }
            watermark = Math.max(watermark, result.getRecord().getOffset());
        }
        return watermark;
    }

    @Override
    protected List<RetryRecord> fetchRetryEntries() {
        // 全量按 offset 升序取：同 key 顺序由读取顺序 + 按序投递 + 失败中断保证；
        // 退避不读 retryAt 字段，由内存时点（nextRetryAt）节制
        return this.retryStore.list(this.name, this.batchSize);
    }

    @Override
    protected List<List<ConsumerRecord>> group(List<ConsumerRecord> records) {
        return MessageUtils.batchByUniqueKey(records, KeyOrderedDeliverer::keyOf);
    }

    @Override
    protected void onRetryDrained() {
        this.refreshBlockedKeys();
        log.info("[{}] retry store drained, {} blocked keys", this.name, this.blockedKeys.size());
    }

    @Override
    protected void onRetryFailed(int failedCount) {
        this.refreshBlockedKeys();
        log.info("[{}] retry aborted with {} failure(s), {} blocked keys",
                this.name, failedCount, this.blockedKeys.size());
    }

    private void refreshBlockedKeys() {
        var keys = this.retryStore.listKeys(this.name);
        this.blockedKeys.clear();
        this.blockedKeys.addAll(keys);
    }

    private boolean isBlocked(ConsumerRecord record) {
        return this.blockedKeys.contains(keyOf(record));
    }

    /**
     * businessKey 归一化：null 视为 ""。与批量分组及仓储落库（NOT NULL 列）保持一致，
     * 否则 null key 消息既不阻塞也不分流，key 内顺序无从保证。
     */
    private static String keyOf(ConsumerRecord record) {
        var key = record.getMessage().getBusinessKey();
        return key != null ? key : "";
    }

}
