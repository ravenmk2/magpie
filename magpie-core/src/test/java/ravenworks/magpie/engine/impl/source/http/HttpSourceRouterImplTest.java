package ravenworks.magpie.engine.impl.source.http;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.source.http.HttpMessageContext;
import ravenworks.magpie.engine.api.source.http.NoSubscriberException;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;


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

    @Test
    void resubscribeReplacesOldSubscriber() {
        // 同名重复订阅: 新订阅者替换旧订阅者, 旧订阅者不再收到消息
        var router = new HttpSourceRouterImpl();
        var first = new AtomicReference<HttpMessageContext>();
        var second = new AtomicReference<HttpMessageContext>();
        router.subscribe("src", ctx -> {
            first.set(ctx);
            ctx.result().complete(null);
        });
        router.subscribe("src", ctx -> {
            second.set(ctx);
            ctx.result().complete(null);
        });

        router.publish("src", event()).join();

        assertNull(first.get());
        assertNotNull(second.get());
        assertEquals("e1", second.get().event().getId());
    }

    @Test
    void differentSourceNamesAreIsolated() {
        // 不同 source name 的订阅互不影响: 发布只派发给同名订阅者
        var router = new HttpSourceRouterImpl();
        var seenA = new AtomicReference<HttpMessageContext>();
        var seenB = new AtomicReference<HttpMessageContext>();
        router.subscribe("src-a", ctx -> {
            seenA.set(ctx);
            ctx.result().complete(null);
        });
        router.subscribe("src-b", ctx -> {
            seenB.set(ctx);
            ctx.result().complete(null);
        });

        router.publish("src-a", event()).join();
        assertEquals("src-a", seenA.get().source());
        assertNull(seenB.get());

        router.publish("src-b", event()).join();
        assertEquals("src-b", seenB.get().source());
    }

}
