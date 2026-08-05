package ravenworks.magpie.engine.api.sink;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface SinkHandler {

    CompletableFuture<SinkResult> handle(ConsumerRecord record);

    CompletableFuture<List<SinkResult>> handle(List<ConsumerRecord> records);

    CompletableFuture<Void> shutdown();

}
