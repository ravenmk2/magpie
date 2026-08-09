package ravenworks.magpie.engine.impl.source.http;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.SpecVersion;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.api.source.http.NoSubscriberException;
import ravenworks.magpie.engine.api.source.http.PublishFailedException;
import ravenworks.magpie.engine.api.source.http.TopicNotAllowedException;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;


class HttpSourceConnectorTest {

    private static final String VALID_ID = "0123456789abcdef0123456789abcdef";


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
                                                    StreamProducer producer,
                                                    String... allowedTopics) {
        return new HttpSourceConnector(router, producer, "src",
                Map.of("allowedTopics", List.of(allowedTopics)));
    }

    private static CloudEvent fullEvent(String subject) {
        return CloudEventBuilder.v1()
                .withId(VALID_ID)
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
        assertEquals(VALID_ID, record.getId());
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
    void missingBusinessKeyIsAccepted() {
        // xbusinesskey 是可选扩展：缺省时应正常发布，businessKey 为 null
        //（路由分区与消息头在生产端归一为空串，见 PartitionUtils/RabbitStreamProducer）
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        var event = CloudEventBuilder.v1()
                .withId(VALID_ID)
                .withSource(URI.create("test"))
                .withType("t.ping")
                .withSubject("orders")
                .build();

        router.publish("src", event).join();

        assertEquals(1, producer.sent.size());
        assertNull(producer.sent.get(0).getBusinessKey());
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
    void conformingUuid7IdIsPassedThrough() {
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        var event = CloudEventBuilder.v1()
                .withId(VALID_ID)
                .withSource(URI.create("test"))
                .withType("t.ping")
                .withSubject("orders")
                .build();
        router.publish("src", event).join();
        assertEquals(VALID_ID, producer.sent.get(0).getId());
    }

    @Test
    void nonConformingIdIsReplacedWithUuid7() {
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

        var id = producer.sent.get(0).getId();
        assertNotEquals("placeholder", id);
        assertTrue(id.matches("[0-9a-f]{32}"), "id must be 32-char hex, got: " + id);
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

    /** send() 不返回失败 future，而是同步抛异常的 producer */
    static class ThrowingStreamProducer implements StreamProducer {

        final RuntimeException error = new RuntimeException("sync boom");

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            throw this.error;
        }

        @Override
        public void close() {
        }

    }

    /** 包装事件：额外暴露一个值为 null 的扩展属性 */
    private static CloudEvent withNullExtension(CloudEvent delegate, String name) {
        return new CloudEvent() {
            @Override
            public CloudEventData getData() {
                return delegate.getData();
            }

            @Override
            public SpecVersion getSpecVersion() {
                return delegate.getSpecVersion();
            }

            @Override
            public String getId() {
                return delegate.getId();
            }

            @Override
            public URI getSource() {
                return delegate.getSource();
            }

            @Override
            public String getType() {
                return delegate.getType();
            }

            @Override
            public String getDataContentType() {
                return delegate.getDataContentType();
            }

            @Override
            public URI getDataSchema() {
                return delegate.getDataSchema();
            }

            @Override
            public String getSubject() {
                return delegate.getSubject();
            }

            @Override
            public OffsetDateTime getTime() {
                return delegate.getTime();
            }

            @Override
            public Object getAttribute(String attributeName) {
                return delegate.getAttribute(attributeName);
            }

            @Override
            public Object getExtension(String extensionName) {
                return name.equals(extensionName) ? null : delegate.getExtension(extensionName);
            }

            @Override
            public Set<String> getExtensionNames() {
                var names = new LinkedHashSet<>(delegate.getExtensionNames());
                names.add(name);
                return names;
            }
        };
    }

    @Test
    void synchronousSendThrowFailsPublish() {
        // send() 同步抛异常（而非失败 future）：走 onMessage 的 catch 兜底分支
        var router = new HttpSourceRouterImpl();
        var producer = new ThrowingStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("orders")).join());
        var cause = assertInstanceOf(PublishFailedException.class, ex.getCause());
        assertSame(producer.error, cause.getCause());
        assertEquals("sync boom", cause.getMessage());
    }

    @Test
    void nullSendResultFailsWithUnknownError() {
        // future 正常完成但 sendResult 为 null：按 "unknown error" 失败
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        producer.thenReturn(CompletableFuture.completedFuture(null));
        var connector = newConnector(router, producer, "orders");
        connector.start();

        var ex = assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("orders")).join());
        var cause = assertInstanceOf(PublishFailedException.class, ex.getCause());
        assertEquals("unknown error", cause.getMessage());
    }

    @Test
    void missingTimeAndDataDefaultToNowAndEmptyPayload() {
        // 事件缺 time / data：eventTime 回落为当前时间，payload 为 byte[0]
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();
        var event = CloudEventBuilder.v1()
                .withId(VALID_ID)
                .withSource(URI.create("test"))
                .withType("t.ping")
                .withSubject("orders")
                .build();

        var before = LocalDateTime.now();
        router.publish("src", event).join();
        var after = LocalDateTime.now();

        var record = producer.sent.get(0);
        assertNotNull(record.getEventTime());
        assertFalse(record.getEventTime().isBefore(before));
        assertFalse(record.getEventTime().isAfter(after));
        assertArrayEquals(new byte[0], record.getPayload());
    }

    @Test
    void nullExtensionValueIsSkippedInHeaders() {
        // 值为 null 的扩展属性应被跳过，不进 headers，也不 NPE
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "orders");
        connector.start();

        router.publish("src", withNullExtension(fullEvent("orders"), "xnull")).join();

        var headers = producer.sent.get(0).getHeaders();
        assertFalse(headers.containsKey("xnull"));
        assertEquals("c1", headers.get("custom"));
    }

    @Test
    void multiStarWildcardMatchesAcrossSegments() {
        // a.*.* 需要 a. 之后两段：a.b.c 命中，a.b 与 ax.b.c 不命中
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "a.*.*");
        connector.start();

        router.publish("src", fullEvent("a.b.c")).join();
        assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("a.b")).join());
        assertThrows(CompletionException.class,
                () -> router.publish("src", fullEvent("ax.b.c")).join());
        assertEquals(1, producer.sent.size());
    }

    @Test
    void loneStarWildcardMatchesAnyTopic() {
        // 单独的 * 编译为 .*：匹配任意 topic
        var router = new HttpSourceRouterImpl();
        var producer = new FakeStreamProducer();
        var connector = newConnector(router, producer, "*");
        connector.start();

        router.publish("src", fullEvent("anything.at.all")).join();
        router.publish("src", fullEvent("orders")).join();
        assertEquals(2, producer.sent.size());
    }

}
