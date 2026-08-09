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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;


/**
 * 使用 RetryStore 的投递模式基类（KEY_ORDERED / BEST_EFFORT）：失败消息先落库再推进水位
 * （最少一次）；流空闲时全量排空到期重试项，遇失败立即中断、等下一个空闲窗口继续。
 * 子类通过 {@link #group} 与重试结果钩子表达模式差异。
 *
 * <p>重试时点由内存态 {@link #nextRetryAt} 节制：重试失败按失败项退避后的 retryAt 延期，
 * 到期前 {@link #canRetry()} 纯内存判断、不打存储；新落库消息立即到期。
 *
 * @author Raven
 */
@Slf4j
public abstract class RetryingDeliverer implements Deliverer {

    /**
     * 无到期项但库非空（全部退避中）时的再探间隔：覆盖内存时点信息不全的场景
     * （外部调整 retryAt 等），只查库不投递，代价远低于直接重试
     */
    private static final Duration RETRY_PROBE_DELAY = Duration.ofSeconds(1);

    /**
     * 重试退避：RETRY_DELAY_MS × 2^(attempts-1)，封顶 RETRY_MAX_DELAY_MS，无次数上限（无 DLQ）
     */
    private static final long RETRY_DELAY_MS = 5_000;
    private static final long RETRY_MAX_DELAY_MS = 300_000;

    protected final String name;
    protected final SinkHandler handler;
    protected final int batchSize;
    protected final CircuitBreaker circuitBreaker;
    protected final RetryMessageStore retryStore;

    protected boolean hasRetryable;
    /**
     * 下次允许发起重试排空的时间点（与存储一致用本地时间比较）；confined 在 WorkLoop 线程
     */
    private LocalDateTime nextRetryAt = LocalDateTime.MIN;
    /**
     * 停机标志：由 interrupt 从停机线程写入，落库重试与重试排空循环据此放弃
     */
    private volatile boolean shutdownRequested;

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
    public void init() {
        this.shutdownRequested = false;
        // 启动即探测存量（含重启后遗留），有则在首个空闲窗口排空
        this.hasRetryable = !this.retryStore.list(this.name, 1).isEmpty();
    }

    @Override
    public void interrupt() {
        this.shutdownRequested = true;
    }

    @Override
    public boolean isReady() {
        return !this.circuitBreaker.isOpen();
    }

    @Override
    public boolean canRetry() {
        // 纯内存判断：空闲期每次空轮询都会调用，不打存储
        return this.hasRetryable && !LocalDateTime.now().isBefore(this.nextRetryAt);
    }

    /**
     * 重试排空：循环取待重投条目重投，全部成功则继续取下一批，直到取空；
     * 任一批有失败立即中断，重试时点按失败项退避延期。
     */
    @Override
    public void retry() {
        while (!this.shutdownRequested) {
            var entries = this.fetchRetryEntries();
            if (entries.isEmpty()) {
                this.onFetchEmpty();
                return;
            }
            if (!this.retryEntries(entries)) {
                return;
            }
        }
    }

    /**
     * 取一批待重投条目。KEY_ORDERED 用 list（全量、offset 升序，同 key 顺序由读取顺序保证，
     * 退避靠内存时点）；BEST_EFFORT 用 listRetryable（按 retryAt 到期过滤，无顺序要求）。
     */
    protected abstract List<RetryRecord> fetchRetryEntries();

    /**
     * 取空时回调：默认视为库已排空；按 retryAt 过滤的模式需覆写——取空可能只是全部退避中。
     */
    protected void onFetchEmpty() {
        this.hasRetryable = false;
        this.onRetryDrained();
    }

    /**
     * 推后重试时点：用于"库非空但本轮取不到可投条目"的探活节拍。
     */
    protected void postponeRetry() {
        this.nextRetryAt = LocalDateTime.now().plus(RETRY_PROBE_DELAY);
    }

    /**
     * 退避时长：RETRY_DELAY_MS × 2^(attempts-1)，封顶 RETRY_MAX_DELAY_MS。
     * long 移位按 64 取模且乘法可能溢出，统一提前封顶。
     */
    protected long retryDelayMillis(int attempts) {
        int shift = Math.min(Math.max(attempts - 1, 0), 62);
        long multiplier = 1L << shift;
        if (RETRY_DELAY_MS > Long.MAX_VALUE / multiplier) {
            return RETRY_MAX_DELAY_MS;
        }
        return Math.min(RETRY_DELAY_MS * multiplier, RETRY_MAX_DELAY_MS);
    }

    /**
     * 重投一批条目：全部成功返回 true；有失败则按退避计算新 retryAt 落库、
     * 按最早失败项的 retryAt 延期重试时点，并中断本轮排空。
     */
    private boolean retryEntries(List<RetryRecord> entries) {
        Map<Long, RetryRecord> entryByOffset = new HashMap<>();
        List<ConsumerRecord> records = new ArrayList<>();
        for (var e : entries) {
            entryByOffset.put(e.getOffset(), e);
            records.add(toConsumerRecord(e));
        }

        int failedCount = 0;
        LocalDateTime earliestFailedRetryAt = null;
        for (var group : this.group(records)) {
            List<SinkResult> results = this.handler.handle(group).join();
            for (var result : results) {
                RetryRecord entry = entryByOffset.get(result.getRecord().getOffset());
                if (entry == null) {
                    continue;
                }
                if (result.getStatus() == SinkStatus.SUCCESS) {
                    this.retryStore.succeeded(entry.getId());
                } else {
                    // 退避由 Deliverer 计算（attempts+1 次失败），存储只负责记录
                    var retryAt = LocalDateTime.now()
                            .plus(Duration.ofMillis(this.retryDelayMillis(entry.getAttempts() + 1)));
                    this.retryStore.failed(entry.getId(), retryAt);
                    if (earliestFailedRetryAt == null || retryAt.isBefore(earliestFailedRetryAt)) {
                        earliestFailedRetryAt = retryAt;
                    }
                    log.warn("[{}] retry failed for {}", this.name, entry.getId());
                    failedCount++;
                }
            }
            if (failedCount > 0) {
                break;
            }
        }
        if (failedCount > 0) {
            // 失败项已按退避推后：对齐其 retryAt，退避期内重试不再打扰存储与下游
            this.nextRetryAt = earliestFailedRetryAt != null
                    ? earliestFailedRetryAt
                    : LocalDateTime.now().plus(RETRY_PROBE_DELAY);
            this.onRetryFailed(failedCount);
            return false;
        }
        return true;
    }

    /**
     * 失败消息落库：先落库再推进水位（最少一次）。落库失败原地重试直到成功；
     * 停机中（interrupt 后）放弃并抛出——deliver 随之中断、水位不推进不返回，
     * 该消息既不投递也未落库，等重启从未提交 offset 重投。
     * 落库成功即标记可重试且立即到期（新条目 retryAt 为 now，同 key 押后除外）。
     */
    protected void persist(ConsumerRecord record) {
        while (true) {
            try {
                this.retryStore.save(this.name, record);
                this.hasRetryable = true;
                this.nextRetryAt = LocalDateTime.now();
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
     * 重试库排空时回调（如刷新阻塞集合）。
     */
    protected void onRetryDrained() {
        log.info("[{}] retry store drained", this.name);
    }

    /**
     * 重试遇失败、中断本轮排空时回调。
     */
    protected void onRetryFailed(int failedCount) {
        log.info("[{}] retry aborted with {} failure(s)", this.name, failedCount);
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
