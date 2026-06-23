package ravenworks.magpie.engine.source.http;

import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public record HttpMessageContext(String source,
                                 CloudEvent event,
                                 CompletableFuture<Void> result) {

}
