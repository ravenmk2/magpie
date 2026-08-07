package ravenworks.magpie.engine.impl.sink.deliverer;

import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 使用 RetryStore 的投递模式基类（KEY_ORDERED / BEST_EFFORT）：失败消息先落库再推进水位
 * （最少一次），NORMAL / RETRYING 双态切换与重试周期在此实现，
 * 子类通过 {@link #group} 与重试结果钩子表达模式差异。
 *
 * @author Raven
 */
@Slf4j
public abstract class RetryingDeliverer implements Deliverer {

    protected enum State {NORMAL, RETRYING}

    protected static final int EMPTY_POLL_THRESHOLD = 5;

    protected State state = State.NORMAL;
    protected boolean hasRetryable;
    private int emptyPollCount;

    @Override
    public void onEmptyPoll(SinkContext ctx) {
        if (!this.hasRetryable) {
            return;
        }
        this.emptyPollCount++;
        if (this.emptyPollCount >= EMPTY_POLL_THRESHOLD) {
            if (!ctx.retryStore().listRetryable(ctx.name(), 1).isEmpty()) {
                this.state = State.RETRYING;
                log.info("[{}] entering RETRYING mode", ctx.name());
            }
            this.emptyPollCount = 0;
        }
    }

    @Override
    public boolean retryPending() {
        return this.state == State.RETRYING;
    }

    @Override
    public void retryCycle(SinkContext ctx) {
        // 只取已到期的重试项（按 offset 升序）；未到期项留在库中等待退避结束
        var entries = ctx.retryStore().listRetryable(ctx.name(), ctx.batchSize());
        if (entries.isEmpty()) {
            // 没有到期项不代表存储为空：退避中的消息仍在，hasRetryable 不能复位
            this.hasRetryable = !ctx.retryStore().list(ctx.name(), 1).isEmpty();
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            this.onRetryDrained(ctx);
            return;
        }

        Map<Long, RetryRecord> entryByOffset = new HashMap<>();
        List<ConsumerRecord> records = new ArrayList<>();
        for (var e : entries) {
            entryByOffset.put(e.getOffset(), e);
            records.add(toConsumerRecord(e));
        }

        int failedCount = 0;
        for (var group : group(records)) {
            List<SinkResult> results = ctx.handler().handle(group).join();
            for (var result : results) {
                RetryRecord entry = entryByOffset.get(result.getRecord().getOffset());
                if (entry == null) {
                    continue;
                }
                if (result.getStatus() == SinkStatus.SUCCESS) {
                    ctx.retryStore().succeeded(entry.getId());
                } else {
                    ctx.retryStore().failed(entry.getId());
                    log.warn("[{}] retry failed for {}", ctx.name(), entry.getId());
                    failedCount++;
                }
            }
            if (failedCount > 0) {
                break;
            }
        }
        if (failedCount > 0) {
            this.state = State.NORMAL;
            this.onRetryFailed(ctx, failedCount);
        }
    }

    /** 把待重投记录按模式切成投递分组（BEST_EFFORT 整批；KEY_ORDERED 按 key 切子批）。 */
    protected abstract List<List<ConsumerRecord>> group(List<ConsumerRecord> records);

    /** 重试库排空、退出 RETRYING 时回调（如刷新阻塞集合）。 */
    protected void onRetryDrained(SinkContext ctx) {
        log.info("[{}] exiting RETRYING mode", ctx.name());
    }

    /** 重试周期出现失败、退回 NORMAL 时回调。 */
    protected void onRetryFailed(SinkContext ctx, int failedCount) {
        log.info("[{}] retry batch had {} failure(s), exiting RETRYING mode", ctx.name(), failedCount);
    }

    protected static ConsumerRecord toConsumerRecord(RetryRecord e) {
        return new ConsumerRecord()
                .setOffset(e.getOffset())
                .setId(e.getMessageId())
                .setType(e.getType())
                .setEventTime(e.getEventTime())
                .setTopic(e.getTopic())
                .setTenantId(e.getTenantId())
                .setBusinessKey(e.getBusinessKey())
                .setHeaders(e.getHeaders())
                .setPayload(e.getPayload());
    }

}
