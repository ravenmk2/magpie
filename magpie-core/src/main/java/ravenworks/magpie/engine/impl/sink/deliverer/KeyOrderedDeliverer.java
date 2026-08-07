package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.extern.slf4j.Slf4j;
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

    @Override
    public void onStart(SinkContext ctx) {
        this.blockedKeys.addAll(ctx.retryStore().listKeys(ctx.name()));
        if (!this.blockedKeys.isEmpty()) {
            this.state = State.RETRYING;
            log.info("[{}] entering RETRYING mode, {} blocked keys",
                    ctx.name(), this.blockedKeys.size());
        }
    }

    @Override
    public void onBatch(List<ConsumerRecord> records, SinkContext ctx) {
        var remaining = filterBlocked(records, ctx);
        if (remaining.isEmpty()) {
            return;
        }
        for (var subBatch : MessageUtils.batchByUniqueKey(remaining, KeyOrderedDeliverer::keyOf)) {
            processSubBatch(subBatch, ctx);
        }
    }

    private List<ConsumerRecord> filterBlocked(List<ConsumerRecord> batch, SinkContext ctx) {
        if (this.blockedKeys.isEmpty()) {
            return batch;
        }
        List<ConsumerRecord> remaining = new ArrayList<>();
        for (var record : batch) {
            if (isBlocked(record)) {
                ctx.persist(record);
                this.hasRetryable = true;
                ctx.advance(record.getOffset());
            } else {
                remaining.add(record);
            }
        }
        return remaining;
    }

    private void processSubBatch(List<ConsumerRecord> subBatch, SinkContext ctx) {
        List<ConsumerRecord> toSend = new ArrayList<>();
        for (var record : subBatch) {
            if (isBlocked(record)) {
                ctx.persist(record);
                this.hasRetryable = true;
                ctx.advance(record.getOffset());
            } else {
                toSend.add(record);
            }
        }
        if (toSend.isEmpty()) {
            return;
        }
        List<SinkResult> results = ctx.handler().handle(toSend).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                ctx.persist(result.getRecord());
                this.hasRetryable = true;
                this.blockedKeys.add(keyOf(result.getRecord()));
            }
            ctx.advance(result.getRecord().getOffset());
        }
    }

    @Override
    protected List<List<ConsumerRecord>> group(List<ConsumerRecord> records) {
        return MessageUtils.batchByUniqueKey(records, KeyOrderedDeliverer::keyOf);
    }

    @Override
    protected void onRetryDrained(SinkContext ctx) {
        refreshBlockedKeys(ctx);
        log.info("[{}] exiting RETRYING mode, {} blocked keys", ctx.name(), this.blockedKeys.size());
    }

    @Override
    protected void onRetryFailed(SinkContext ctx, int failedCount) {
        refreshBlockedKeys(ctx);
        log.info("[{}] retry batch had {} failure(s), exiting RETRYING mode, {} blocked keys",
                ctx.name(), failedCount, this.blockedKeys.size());
    }

    private void refreshBlockedKeys(SinkContext ctx) {
        var keys = ctx.retryStore().listKeys(ctx.name());
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
        return record.getBusinessKey() != null ? record.getBusinessKey() : "";
    }

}
