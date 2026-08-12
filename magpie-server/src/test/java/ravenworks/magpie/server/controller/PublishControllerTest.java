package ravenworks.magpie.server.controller;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import ravenworks.magpie.engine.api.source.http.*;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.impl.source.http.HttpSourceConnector;
import ravenworks.magpie.engine.impl.source.http.HttpSourceRouterImpl;
import ravenworks.magpie.server.dto.ApiResponse;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;


/**
 * 不启动 MVC 容器，直接构造控制器并断言 CompletableFuture 的错误映射矩阵。
 */
class PublishControllerTest {

    private static final CloudEvent EVENT = CloudEventBuilder.v1()
            .withId("id-1")
            .withType("test.event")
            .withSource(URI.create("https://example.com/source"))
            .build();

    private static HttpSourceRouter failingRouter(Throwable error) {
        return new HttpSourceRouter() {

            @Override
            public CompletableFuture<Void> publish(String source, CloudEvent event) {
                return CompletableFuture.failedFuture(error);
            }

            @Override
            public void subscribe(String source, Consumer<HttpMessageContext> consumer) {
            }

            @Override
            public void unsubscribe(String source) {
            }
        };
    }

    private static HttpSourceRouter okRouter() {
        return new HttpSourceRouter() {

            @Override
            public CompletableFuture<Void> publish(String source, CloudEvent event) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void subscribe(String source, Consumer<HttpMessageContext> consumer) {
            }

            @Override
            public void unsubscribe(String source) {
            }
        };
    }

    @Test
    void successReturnsOkEnvelope() {
        var controller = new PublishController(okRouter());

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertTrue(body.success());
        assertEquals(Map.of(), body.data());
        assertNull(body.error());
    }

    @Test
    void topicNotAllowedMapsToForbidden() {
        var controller = new PublishController(failingRouter(new TopicNotAllowedException("orders")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("topic_not_allowed_error", body.error().code());
        assertNotNull(body.error().message());
    }

    @Test
    void noSubscriberMapsToServiceUnavailable() {
        var controller = new PublishController(failingRouter(new NoSubscriberException("orders")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("no_subscriber_error", body.error().code());
    }

    @Test
    void unknownErrorMapsToBadGateway() {
        var controller = new PublishController(failingRouter(new IllegalStateException("boom")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("publish_failed_error", body.error().code());
        assertEquals("boom", body.error().message());
    }

    @Test
    void completionExceptionIsUnwrapped() {
        var controller = new PublishController(failingRouter(
                new CompletionException(new TopicNotAllowedException("orders"))));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("topic_not_allowed_error", body.error().code());
    }

    @Test
    void completionExceptionWithoutCauseMapsToBadGateway() {
        var controller = new PublishController(failingRouter(new CompletionException("bare", null)));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("publish_failed_error", body.error().code());
        assertEquals("bare", body.error().message());
    }

    @Test
    void invalidMessageMapsToBadRequest() {
        var controller = new PublishController(
                failingRouter(new InvalidMessageException("xbusinesskey", 257, 256)));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("invalid_message_error", body.error().code());
        assertNotNull(body.error().message());
    }

    /**
     * 真实路由 + 真实 HTTP source 连接器接线的端到端入口校验：
     * 超长字段在 publish 进入 stream 前被拒绝，映射为 400。
     */
    private static PublishController wiredController() {
        var router = new HttpSourceRouterImpl();
        var connector = new HttpSourceConnector(router, new FakeStreamProducer(), "src",
                Map.of("allowedTopics", List.of("*")));
        connector.start();
        return new PublishController(router);
    }

    static class FakeStreamProducer implements StreamProducer {

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            return CompletableFuture.completedFuture(
                    new SendResult().setSucceeded(true).setMessage(record));
        }

        @Override
        public void close() {
        }

    }

    @Test
    void oversizedIdMapsToBadRequest() {
        var controller = wiredController();
        var event = CloudEventBuilder.v1()
                .withId("i".repeat(33))
                .withType("test.event")
                .withSource(URI.create("https://example.com/source"))
                .withSubject("orders")
                .build();

        ResponseEntity<Object> response = controller.publish("src", event).join();

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("invalid_message_error", body.error().code());
    }

    @Test
    void oversizedBusinessKeyMapsToBadRequest() {
        var controller = wiredController();
        var event = CloudEventBuilder.v1()
                .withId("0123456789abcdef0123456789abcdef")
                .withType("test.event")
                .withSource(URI.create("https://example.com/source"))
                .withSubject("orders")
                .withExtension("xbusinesskey", "b".repeat(257))
                .build();

        ResponseEntity<Object> response = controller.publish("src", event).join();

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("invalid_message_error", body.error().code());
    }

    @Test
    void boundaryLengthsAreAccepted() {
        // 边界值：id 恰好 32、type/businessKey 恰好 256，放行并正常发布
        var controller = wiredController();
        var event = CloudEventBuilder.v1()
                .withId("0123456789abcdef0123456789abcdef")
                .withType("t".repeat(256))
                .withSource(URI.create("https://example.com/source"))
                .withSubject("orders")
                .withExtension("xbusinesskey", "b".repeat(256))
                .build();

        ResponseEntity<Object> response = controller.publish("src", event).join();

        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void unreadableBodyMapsToBadRequest() {
        var controller = new PublishController(okRouter());
        HttpInputMessage inputMessage = new HttpInputMessage() {

            @Override
            public InputStream getBody() {
                return InputStream.nullInputStream();
            }

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }
        };

        ResponseEntity<Object> response = controller.onUnreadable(
                new HttpMessageNotReadableException("no body", inputMessage));

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        ApiResponse<?> body = assertInstanceOf(ApiResponse.class, response.getBody());
        assertFalse(body.success());
        assertNull(body.data());
        assertEquals("invalid_request_error", body.error().code());
        assertEquals("no body", body.error().message());
    }

}
