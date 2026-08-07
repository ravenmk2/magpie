package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;


/**
 * 投递处置角色：按 DeliveryMode 决定每一批消息如何处置（怎么投、失败怎么办、何时重试），
 * 实际发送委托给构造时注入的 SinkHandler 完成。
 *
 * <p>由 SinkWorker 单向驱动：除 {@link #onShutdown()} 由停机线程调用外，
 * 所有回调都运行在 Worker 的 WorkLoop 线程上，实现类内部无需同步。
 * Deliverer 只能由 SinkWorker 驱动——绕过 Worker 使用必须自行保证 onShutdown 被调用，
 * 否则长循环（原地重试、落库重试）失去中断能力。
 *
 * @author Raven
 */
public interface Deliverer {

    /**
     * Worker 启动后回调（consumer.start 之后、首次拉取之前）。
     */
    default void onStart() {
    }

    /**
     * 本周期该做什么：拉取新消息 / 执行重试 / 等待（熔断开启，Worker 不拉取、空转一轮）。
     */
    Action nextAction();

    /**
     * 处置一批消息，返回已处置（投递成功或已落库）的最大 offset；本批无进展返回 -1。
     *
     * <p>契约：返回值是已处置前缀的水位——批次按 offset 有序，所有 offset ≤ 返回值的记录
     * 必须都已处置。返回部分水位（小于本批最大 offset）即中断信号：Worker 提交前缀后
     * 不再拉取，未处置后缀等重启从未提交处重投。
     */
    long onBatch(List<ConsumerRecord> records);

    /**
     * 拉取为空时回调：重试类模式在此累计空闲并决定是否进入重试。
     */
    default void onEmptyPoll() {
    }

    /**
     * 执行一个重试周期：从 RetryStore 取到期项重投。
     */
    default void retryCycle() {
    }

    /**
     * 停机提交前钳制水位：重试类模式不越过未落库缺口，其余模式原样返回。
     */
    default long clampCommit(long watermark) {
        return watermark;
    }

    /**
     * Worker 停机时主动通知：长循环（原地重试、落库重试）应尽快退出。由停机线程调用。
     */
    default void onShutdown() {
    }

    enum Action {POLL, RETRY, WAIT}

}
