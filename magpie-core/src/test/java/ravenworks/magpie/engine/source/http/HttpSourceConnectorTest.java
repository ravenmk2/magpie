package ravenworks.magpie.engine.source.http;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.SendResult;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpSourceConnectorTest {

    static class FakeStreamProducer implements StreamProducer {

        final List<MessageRecord> sent = new CopyOnWriteArrayList<>();
        private final Queue<CompletableFuture<SendResult>> script = new ConcurrentLinkedQueue<>();

        void thenReturn(CompletableFuture<SendResult> result) {
            this.script.add(result);
        }

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            this.sent.add(record);
            var result = this.script.poll();
            return result != null
                    ? result
                    : CompletableFuture.completedFuture(new SendResult().setSucceeded(true).setMessage(record));
        }

        @Override
        public void close() {
        }

    }

    private static HttpSourceConnector newConnector(HttpSourceRouter router,
                                                    FakeStreamProducer producer,
                                                    String... allowedTopics) {
        return new HttpSourceConnector(router, producer, "src",
                Map.of("allowedTopics", List.of(allowedTopics)));
    }

    private static CloudEvent fullEvent(String subject) {
        return CloudEventBuilder.v1()
                .withId("e1")
                .withSource(URI.create("test"))
                .withType("t.order.created")
                .withSubject(subject)
                .withTime(OffsetDateTime.now())
                .withData("application/json", "{\"a\":1}".getBytes(StandardCharsets.UTF_8))
                .withExtension("xtenantid", "t1")
                .withExtension("xbusinesskey", "bk1")
                .withExtension("custom", "c1")
                .build();
    }

    @Test
    void allowedTopicIsSentAndCompletesPublish() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders", "payments.*");
        connector.start();

        router.publish("src", fullEvent("orders")).join();

        assertEquals(1, producer.sent.size());
        var record = producer.sent.get(0);
        assertEquals("e1", record.getId());
        assertEquals("t.order.created", record.getType());
        assertEquals("orders", record.getTopic());
        assertEquals("t1", record.getTenantId());
        assertEquals("bk1", record.getBusinessKey());
        assertArrayEquals("{\"a\":1}".getBytes(StandardCharsets.UTF_8), record.getPayload());
        assertEquals(Map.of("custom", "c1"), record.getHeaders());
        assertNotNull(record.getEventTime());
    }

    @Test
    void notAllowedTopicFailsWithTopicNotAllowed() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("other")).join());
        assertInstanceOf(TopicNotAllowedException.class, ex.getCause());
        assertTrue(producer.sent.isEmpty());
    }

    @Test
    void blankSubjectFailsWithTopicNotAllowed() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        var event = CloudEventBuilder.v1()
                .withId("e2")
                .withSource(URI.create("test"))
                .withType("t.ping")
                .build();

        var ex = assertThrows(CompletionException.class, () -> router.publish("src", event).join());
        assertInstanceOf(TopicNotAllowedException.class, ex.getCause());
        assertTrue(producer.sent.isEmpty());
    }

    @Test
    void wildcardTopicMatches() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "payments.*");
        connector.start();

        router.publish("src", fullEvent("payments.created")).join();
        assertEquals(1, producer.sent.size());
    }

    @Test
    void wildcardTopicDoesNotOvermatch() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "payments.*");
        connector.start();

        assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("paymentsx.created")).join());
        assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("payments")).join());
        assertTrue(producer.sent.isEmpty());
    }

    @Test
    void sendFailureFailsPublish() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        producer.thenReturn(CompletableFuture.failedFuture(new RuntimeException("io boom")));
        var connector = newConnector(router, producer, "orders");
        connector.start();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("orders")).join());
        assertInstanceOf(PublishFailedException.class, ex.getCause());
    }

    @Test
    void unsuccessfulSendResultFailsPublish() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        producer.thenReturn(CompletableFuture.completedFuture(
                new SendResult().setSucceeded(false).setError("broker rejected")));
        var connector = newConnector(router, producer, "orders");
        connector.start();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("orders")).join());
        assertInstanceOf(PublishFailedException.class, ex.getCause());
    }

    @Test
    void eventIdIsPassedThrough() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        var event = CloudEventBuilder.v1()
                .withId("placeholder")
                .withSource(URI.create("test"))
                .withType("t.ping")
                .withSubject("orders")
                .build();
        router.publish("src", event).join();
        assertEquals("placeholder", producer.sent.get(0).getId());
    }

    @Test
    void shutdownUnsubscribes() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        connector.shutdown().join();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("orders")).join());
        assertInstanceOf(NoSubscriberException.class, ex.getCause());
        assertTrue(producer.sent.isEmpty());
    }

}
