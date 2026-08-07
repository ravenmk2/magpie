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
import ravenworks.magpie.engine.api.stream.MessageRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;


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

    protected final String name;
    protected final SinkHandler handler;
    protected final int batchSize;
    protected final CircuitBreaker circuitBreaker;
    protected final RetryMessageStore retryStore;

    protected State state = State.NORMAL;
    protected boolean hasRetryable;
    private int emptyPollCount;
    /**
     * 停机标志：由 onShutdown 从停机线程写入，落库重试循环据此放弃
     */
    private volatile boolean shutdownRequested;
    /**
     * 停机时落库失败的最小 offset：停机提交不得越过它（at-least-once 缺口防护）
     */
    private long firstUnpersistedOffset = Long.MAX_VALUE;

    protected RetryingDeliverer(@NonNull String name,
                                @NonNull SinkHandler handler,
                                int batchSize,
                                @NonNull CircuitBreaker circuitBreaker,
                                @NonNull RetryMessageStore retryStore) {
        this.name = name;
        this.handler = handler;
        this.batchSize = batchSize;
        this.circuitBreaker = circuitBreaker;
        this.retryStore = retryStore;
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
        if (this.circuitBreaker.isOpen()) {
            return Action.WAIT;
        }
        return this.state == State.RETRYING ? Action.RETRY : Action.POLL;
    }

    @Override
    public void onEmptyPoll() {
        if (!this.hasRetryable) {
            return;
        }
        this.emptyPollCount++;
        if (this.emptyPollCount >= EMPTY_POLL_THRESHOLD) {
            if (!this.retryStore.listRetryable(this.name, 1).isEmpty()) {
                this.state = State.RETRYING;
                log.info("[{}] entering RETRYING mode", this.name);
            }
            this.emptyPollCount = 0;
        }
    }

    @Override
    public void retryCycle() {
        // 只取已到期的重试项（按 offset 升序）；未到期项留在库中等待退避结束
        var entries = this.retryStore.listRetryable(this.name, this.batchSize);
        if (entries.isEmpty()) {
            // 没有到期项不代表存储为空：退避中的消息仍在，hasRetryable 不能复位
            this.hasRetryable = !this.retryStore.list(this.name, 1).isEmpty();
            this.emptyPollCount = 0;
            this.state = State.NORMAL;
            this.onRetryDrained();
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
            List<SinkResult> results = this.handler.handle(group).join();
            for (var result : results) {
                RetryRecord entry = entryByOffset.get(result.getRecord().getOffset());
                if (entry == null) {
                    continue;
                }
                if (result.getStatus() == SinkStatus.SUCCESS) {
                    this.retryStore.succeeded(entry.getId());
                } else {
                    this.retryStore.failed(entry.getId());
                    log.warn("[{}] retry failed for {}", this.name, entry.getId());
                    failedCount++;
                }
            }
            if (failedCount > 0) {
                break;
            }
        }
        if (failedCount > 0) {
            this.state = State.NORMAL;
            this.onRetryFailed(failedCount);
        }
    }

    @Override
    public long clampCommit(long watermark) {
        // 提交水位不得越过停机时未能落库的消息：只提交缺口之前，其余等重启重投
        long committable = Math.min(watermark, this.firstUnpersistedOffset - 1);
        if (this.firstUnpersistedOffset <= watermark) {
            log.warn("[{}] clamping shutdown commit to {}: message at offset {} was not persisted",
                    this.name, committable, this.firstUnpersistedOffset);
        }
        return committable;
    }

    /**
     * 失败消息落库：先落库再推进水位（最少一次）。落库失败原地重试直到成功；
     * 停机中（onShutdown 后）放弃并抛出，抛出消息的 offset 记为未落库缺口——
     * 停机提交不得越过它，否则该消息既未投递又未落库，却随水位提交被跳过，永久丢失。
     */
    protected void persist(ConsumerRecord record) {
        try {
            saveWithRetry(record);
        } catch (RuntimeException e) {
            this.firstUnpersistedOffset = Math.min(this.firstUnpersistedOffset, record.getOffset());
            throw e;
        }
    }

    private void saveWithRetry(ConsumerRecord record) {
        while (true) {
            try {
                this.retryStore.save(this.name, record);
                return;
            } catch (RuntimeException e) {
                if (this.shutdownRequested) {
                    throw e;
                }
                log.warn("[{}] save retry message failed (offset={}), retry in 1s: {}",
                        this.name, record.getOffset(), e.toString());
                LockSupport.parkNanos(1_000_000_000L);
            }
        }
    }

    /**
     * 把待重投记录按模式切成投递分组（BEST_EFFORT 整批；KEY_ORDERED 按 key 切子批）。
     */
    protected abstract List<List<ConsumerRecord>> group(List<ConsumerRecord> records);

    /**
     * 重试库排空、退出 RETRYING 时回调（如刷新阻塞集合）。
     */
    protected void onRetryDrained() {
        log.info("[{}] exiting RETRYING mode", this.name);
    }

    /**
     * 重试周期出现失败、退回 NORMAL 时回调。
     */
    protected void onRetryFailed(int failedCount) {
        log.info("[{}] retry batch had {} failure(s), exiting RETRYING mode", this.name, failedCount);
    }

    protected static ConsumerRecord toConsumerRecord(RetryRecord e) {
        var message = new MessageRecord()
                .setId(e.getMessageId())
                .setType(e.getType())
                .setEventTime(e.getEventTime())
                .setTopic(e.getTopic())
                .setTenantId(e.getTenantId())
                .setBusinessKey(e.getBusinessKey())
                .setHeaders(e.getHeaders())
                .setPayload(e.getPayload());
        return new ConsumerRecord()
                .setOffset(e.getOffset())
                .setMessage(message);
    }

}
