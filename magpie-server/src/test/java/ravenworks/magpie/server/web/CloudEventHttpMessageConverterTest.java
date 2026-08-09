package ravenworks.magpie.server.web;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.v1.CloudEventV1;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;


/**
 * 直接调用 readInternal/writeInternal（同包可见），用最小 HttpInputMessage 桩代替 spring-test。
 */
class CloudEventHttpMessageConverterTest {

    private final CloudEventHttpMessageConverter converter = new CloudEventHttpMessageConverter();

    private static HttpInputMessage inputMessage(HttpHeaders headers, byte[] body) {
        return new HttpInputMessage() {

            @Override
            public InputStream getBody() {
                return new ByteArrayInputStream(body);
            }

            @Override
            public HttpHeaders getHeaders() {
                return headers;
            }
        };
    }

    private static HttpOutputMessage outputMessage() {
        return new HttpOutputMessage() {

            @Override
            public OutputStream getBody() {
                return new ByteArrayOutputStream();
            }

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }
        };
    }

    @Test
    void readsStructuredJsonMode() throws IOException {
        String json = """
                {
                  "specversion": "1.0",
                  "id": "evt-1",
                  "type": "com.example.order.created",
                  "source": "https://example.com/orders",
                  "datacontenttype": "application/json",
                  "data": {"orderId": 42}
                }
                """;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(new MediaType("application", "cloudevents+json"));

        CloudEvent event = this.converter.readInternal(CloudEvent.class,
                inputMessage(headers, json.getBytes(StandardCharsets.UTF_8)));

        assertEquals("evt-1", event.getId());
        assertEquals("com.example.order.created", event.getType());
        assertEquals("https://example.com/orders", event.getSource().toString());
        assertNotNull(event.getData());
        assertTrue(new String(event.getData().toBytes(), StandardCharsets.UTF_8).contains("\"orderId\":42"));
    }

    @Test
    void readsBinaryMode() throws IOException {
        HttpHeaders headers = new HttpHeaders();
        headers.add("ce-specversion", "1.0");
        headers.add("ce-id", "evt-2");
        headers.add("ce-type", "com.example.ping");
        headers.add("ce-source", "/tests");

        CloudEvent event = this.converter.readInternal(CloudEvent.class,
                inputMessage(headers, "hello".getBytes(StandardCharsets.UTF_8)));

        assertEquals("evt-2", event.getId());
        assertEquals("com.example.ping", event.getType());
        assertEquals("/tests", event.getSource().toString());
        assertNotNull(event.getData());
        assertEquals("hello", new String(event.getData().toBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void rejectsMalformedStructuredBody() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(new MediaType("application", "cloudevents+json"));
        byte[] body = "{\"specversion\":\"1.0\"".getBytes(StandardCharsets.UTF_8);

        assertThrows(HttpMessageNotReadableException.class,
                () -> this.converter.readInternal(CloudEvent.class, inputMessage(headers, body)));
    }

    @Test
    void rejectsBinaryModeWithMissingRequiredHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("ce-specversion", "1.0");
        headers.add("ce-id", "evt-3");
        // 缺少 ce-type 与 ce-source，必须解析失败
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);

        assertThrows(HttpMessageNotReadableException.class,
                () -> this.converter.readInternal(CloudEvent.class, inputMessage(headers, body)));
    }

    @Test
    void supportsOnlyCloudEventClasses() {
        assertTrue(this.converter.supports(CloudEvent.class));
        assertTrue(this.converter.supports(CloudEventV1.class), "CloudEvent subclasses are supported");
        assertFalse(this.converter.supports(String.class));
        assertFalse(this.converter.supports(Object.class));
    }

    @Test
    void canWriteIsAlwaysFalse() {
        assertFalse(this.converter.canWrite(CloudEvent.class, MediaType.APPLICATION_JSON));
        assertFalse(this.converter.canWrite(CloudEvent.class, null));
    }

    @Test
    void writeInternalAlwaysThrows() {
        CloudEvent event = CloudEventBuilder.v1()
                .withId("evt-4")
                .withType("com.example.ping")
                .withSource(java.net.URI.create("/tests"))
                .build();

        assertThrows(HttpMessageNotWritableException.class,
                () -> this.converter.writeInternal(event, outputMessage()));
    }

}
