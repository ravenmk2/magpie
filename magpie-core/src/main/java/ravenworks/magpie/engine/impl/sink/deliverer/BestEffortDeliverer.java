package ravenworks.magpie.engine.impl.sink.deliverer;

import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;


/**
 * BEST_EFFORT：不保证顺序，整批投递追求吞吐；失败消息先落 RetryStore 再推进水位，
 * 最少一次保证与 KEY_ORDERED 一致。启动时先进入 RETRYING 排空存量重试消息。
 *
 * @author Raven
 */
public class BestEffortDeliverer extends RetryingDeliverer {

    @Override
    public void onStart(SinkContext ctx) {
        this.state = State.RETRYING;
    }

    @Override
    public void onBatch(List<ConsumerRecord> records, SinkContext ctx) {
        List<SinkResult> results = ctx.handler().handle(records).join();
        for (var result : results) {
            if (result.getStatus() != SinkStatus.SUCCESS) {
                // 先持久化再推进水位：落库失败的消息不提交 offset，等待重投
                ctx.persist(result.getRecord());
                this.hasRetryable = true;
            }
            ctx.advance(result.getRecord().getOffset());
        }
    }

    @Override
    protected List<List<ConsumerRecord>> group(List<ConsumerRecord> records) {
        return List.of(records);
    }

}
