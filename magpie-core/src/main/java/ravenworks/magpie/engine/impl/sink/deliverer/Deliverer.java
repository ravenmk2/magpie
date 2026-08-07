package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;


/**
 * 投递处置角色：按 DeliveryMode 决定每一批消息如何处置（怎么投、失败怎么办、何时重试），
 * 实际发送通过 {@link SinkContext#handler()} 委托给 SinkHandler 完成。
 *
 * <p>所有回调都运行在 SinkWorker 的 WorkLoop 线程上，实现类内部无需同步。
 *
 * @author Raven
 */
public interface Deliverer {

    /** Worker 启动后回调（consumer.start 之后、首次拉取之前）。 */
    default void onStart(SinkContext ctx) {
    }

    /**
     * 处置新拉取的一批消息。已处置完毕（投递成功或已落库）的记录必须用
     * {@link SinkContext#advance(long)} 记账，Worker 在本方法返回后统一提交水位。
     */
    void onBatch(List<ConsumerRecord> records, SinkContext ctx);

    /** 拉取为空时回调：重试类模式在此累计空闲并决定是否进入重试。 */
    default void onEmptyPoll(SinkContext ctx) {
    }

    /** 本周期是否应执行重试而不是拉取新消息。 */
    default boolean retryPending() {
        return false;
    }

    /** 执行一个重试周期：从 RetryStore 取到期项重投。 */
    default void retryCycle(SinkContext ctx) {
    }

}
