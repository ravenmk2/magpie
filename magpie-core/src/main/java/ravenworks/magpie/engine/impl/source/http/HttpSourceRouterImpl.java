package ravenworks.magpie.engine.impl.source.http;

import io.cloudevents.CloudEvent;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.source.http.HttpMessageContext;
import ravenworks.magpie.engine.api.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.api.source.http.NoSubscriberException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;


/**
 * @author Raven
 */
@Slf4j
public class HttpSourceRouterImpl implements HttpSourceRouter {

    private final Map<String, Consumer<HttpMessageContext>> subscribers = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> publish(@NonNull String source, @NonNull CloudEvent event) {
        Consumer<HttpMessageContext> consumer = this.subscribers.get(source);
        if (consumer == null) {
            return CompletableFuture.failedFuture(new NoSubscriberException(source));
        }
        var result = new CompletableFuture<Void>();
        try {
            consumer.accept(new HttpMessageContext(source, event, result));
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public void subscribe(@NonNull String source, @NonNull Consumer<HttpMessageContext> consumer) {
        this.subscribers.put(source, consumer);
        log.info("HTTP source '{}' subscribed", source);
    }

    @Override
    public void unsubscribe(@NonNull String source) {
        if (this.subscribers.remove(source) != null) {
            log.info("HTTP source '{}' unsubscribed", source);
        }
    }

}
