package ravenworks.magpie.engine.api.source.http;

import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


/**
 * @author Raven
 */
public interface HttpSourceRouter {

    CompletableFuture<Void> publish(String source, CloudEvent event);

    void subscribe(String source, Consumer<HttpMessageContext> consumer);

    void unsubscribe(String source);

}
