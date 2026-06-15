package ravenworks.magpie.engine.sink.http;

import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface HttpSender {

    CompletableFuture<HttpSendResult> send(ConsumerRecord record);

    CompletableFuture<List<HttpSendResult>> send(List<ConsumerRecord> records);

    CompletableFuture<Void> shutdown();

}
