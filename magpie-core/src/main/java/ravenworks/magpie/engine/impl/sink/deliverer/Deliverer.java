package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;


/**
 * 投递处置角色：按 DeliveryMode 决定每一批消息如何处置（怎么投、失败怎么办、何时重试），
 * 实际发送委托给构造时注入的 SinkHandler 完成。
 *
 * <p>由 SinkWorker 单向驱动：除 {@link #interrupt()} 由停机线程调用外，
 * 所有回调都运行在 Worker 的 WorkLoop 线程上，实现类内部无需同步。
 * Deliverer 只能由 SinkWorker 驱动——绕过 Worker 使用必须自行保证 interrupt 被调用，
 * 否则长循环（原地重试、落库重试、重试排空）失去中断能力。
 *
 * @author Raven
 */
public interface Deliverer {

    /**
     * Worker 启动后回调（consumer.start 之后、首次拉取之前）。
     */
    default void init() {
    }

    /**
     * 现在能否拉取新消息：熔断开启期间返回 false，Worker 不拉取、停顿后下轮再探。
     */
    boolean isReady();

    /**
     * 空轮询时检查：现在是否应执行一轮重试排空。空转期间每次空轮询都会调用，
     * 实现必须是纯内存判断（如比对内存态的重试时点），不得访问存储。
     */
    default boolean canRetry() {
        return false;
    }

    /**
     * 处置一批消息，返回已处置前缀的水位与是否处置完毕（见 {@link BatchOutcome}）。
     */
    BatchOutcome deliver(List<ConsumerRecord> records);

    /**
     * 重试排空：从 RetryStore 取到期项重投，成功则继续取下一批直到没有到期项；
     * 任一批有失败立即中断（失败项已按退避推后），等下一个空闲窗口再继续。
     */
    default void retry() {
    }

    /**
     * Worker 停机时主动通知：长循环（原地重试、落库重试、重试排空）应尽快退出。
     * 由停机线程调用，是本接口唯一跨线程的方法。
     */
    default void interrupt() {
    }

}
