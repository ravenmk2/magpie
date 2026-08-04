package ravenworks.magpie.engine.source.http;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpSourceRouterImplTest {

    private static CloudEvent event() {
        return CloudEventBuilder.v1()
                .withId("e1")
                .withSource(URI.create("test"))
                .withType("t.test")
                .withSubject("orders")
                .build();
    }

    @Test
    void publishWithoutSubscriberFailsWithNoSubscriber() {
        var router = new HttpSourceRouterImpl();
        var future = router.publish("src", event());
        var ex = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(NoSubscriberException.class, ex.getCause());
    }

    @Test
    void publishDispatchesContextToSubscriber() {
        var router = new HttpSourceRouterImpl();
        var seen = new AtomicReference<HttpMessageContext>();
        router.subscribe("src", ctx -> {
            seen.set(ctx);
            ctx.result().complete(null);
        });

        router.publish("src", event()).join();

        assertEquals("src", seen.get().source());
        assertEquals("e1", seen.get().event().getId());
    }

    @Test
    void subscriberExceptionFailsResult() {
        var router = new HttpSourceRouterImpl();
        var boom = new RuntimeException("boom");
        router.subscribe("src", ctx -> {
            throw boom;
        });

        var ex = assertThrows(CompletionException.class, () -> router.publish("src", event()).join());
        assertSame(boom, ex.getCause());
    }

    @Test
    void unsubscribeStopsDispatch() {
        var router = new HttpSourceRouterImpl();
        router.subscribe("src", ctx -> ctx.result().complete(null));
        router.unsubscribe("src");

        var ex = assertThrows(CompletionException.class, () -> router.publish("src", event()).join());
        assertInstanceOf(NoSubscriberException.class, ex.getCause());
    }

}
