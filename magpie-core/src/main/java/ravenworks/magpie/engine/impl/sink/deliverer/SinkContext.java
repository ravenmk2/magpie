package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;


/**
 * SinkWorker 提供给 Deliverer 的受限出口。两条 at-least-once 不变式由 Worker 在此看守：
 * 先落库（{@link #persist}）再推进水位（{@link #advance}）；停机提交不越过未能落库的消息。
 *
 * @author Raven
 */
public interface SinkContext {

    String name();

    int batchSize();

    SinkHandler handler();

    CircuitBreaker circuitBreaker();

    RetryMessageStore retryStore();

    /** WorkLoop 是否仍在运行：长循环（原地重试、落库重试）每轮都应检查。 */
    boolean isRunning();

    /** 记录一条已处置消息的 offset，Worker 在批处理后统一提交。 */
    void advance(long offset);

    /**
     * 失败消息落库：原地重试直到成功；停机中放弃并抛出，
     * 抛出消息的 offset 会被记为未落库缺口，停机提交不会越过它。
     */
    void persist(ConsumerRecord record);

}
