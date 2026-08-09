package ravenworks.magpie.server.controller;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import ravenworks.magpie.engine.api.source.http.HttpMessageContext;
import ravenworks.magpie.engine.api.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.api.source.http.NoSubscriberException;
import ravenworks.magpie.engine.api.source.http.TopicNotAllowedException;
import ravenworks.magpie.server.dto.ErrorResponse;

import java.io.InputStream;
import java.net.URI;
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
    void successReturnsOkWithEmptyMap() {
        var controller = new PublishController(okRouter());

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(Map.of(), response.getBody());
    }

    @Test
    void topicNotAllowedMapsToForbidden() {
        var controller = new PublishController(failingRouter(new TopicNotAllowedException("orders")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("topic_not_allowed_error", body.error());
        assertNotNull(body.message());
    }

    @Test
    void noSubscriberMapsToServiceUnavailable() {
        var controller = new PublishController(failingRouter(new NoSubscriberException("orders")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("no_subscriber_error", body.error());
    }

    @Test
    void unknownErrorMapsToBadGateway() {
        var controller = new PublishController(failingRouter(new IllegalStateException("boom")));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("publish_failed_error", body.error());
        assertEquals("boom", body.message());
    }

    @Test
    void completionExceptionIsUnwrapped() {
        var controller = new PublishController(failingRouter(
                new CompletionException(new TopicNotAllowedException("orders"))));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("topic_not_allowed_error", body.error());
    }

    @Test
    void completionExceptionWithoutCauseMapsToBadGateway() {
        var controller = new PublishController(failingRouter(new CompletionException("bare", null)));

        ResponseEntity<Object> response = controller.publish("orders", EVENT).join();

        assertEquals(HttpStatus.BAD_GATEWAY, response.getStatusCode());
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("publish_failed_error", body.error());
        assertEquals("bare", body.message());
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
        ErrorResponse body = assertInstanceOf(ErrorResponse.class, response.getBody());
        assertEquals("invalid_request_error", body.error());
        assertEquals("no body", body.message());
    }

}
