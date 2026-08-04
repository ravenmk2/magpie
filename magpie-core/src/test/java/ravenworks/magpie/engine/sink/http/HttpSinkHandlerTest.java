package ravenworks.magpie.engine.sink.http;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class HttpSinkHandlerTest {

    @Test
    void fixedBackoffAlwaysReturnsDelay() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("fixed", 1_000, 30_000, 1));
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("fixed", 1_000, 30_000, 5));
    }

    @Test
    void unknownBackoffFallsBackToFixed() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("jitter", 1_000, 30_000, 3));
    }

    @Test
    void exponentialDoublesPerAttempt() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 1));
        assertEquals(2_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 2));
        assertEquals(4_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 3));
    }

    @Test
    void exponentialBackoffIsCaseInsensitive() {
        assertEquals(2_000, HttpSinkHandler.computeBackoffDelay("EXPONENTIAL", 1_000, 30_000, 2));
    }

    @Test
    void exponentialIsCappedAtMaxDelay() {
        assertEquals(30_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 10));
    }

    @Test
    void exponentialDoesNotWrapOrOverflowAtHighAttempts() {
        // long 移位按 64 取模，attempt >= 64 时 1L << (attempt - 1) 会回绕，必须仍封顶
        for (int attempt : new int[]{63, 64, 65, 100, Integer.MAX_VALUE}) {
            assertEquals(30_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, attempt),
                    "attempt=" + attempt);
        }
    }

    @Test
    void buildCloudEventMapsAllFields() {
        var record = new ConsumerRecord()
                .setOffset(42L)
                .setId("id-1")
                .setType("t.order.created")
                .setTopic("orders")
                .setEventTime(LocalDateTime.of(2026, 8, 4, 12, 30, 45))
                .setTenantId("tenant-1")
                .setBusinessKey("bk-1")
                .setHeaders(Map.of("h1", "v1"))
                .setPayload("{\"a\":1}".getBytes(StandardCharsets.UTF_8));

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertEquals("id-1", event.getId());
        assertEquals("t.order.created", event.getType());
        assertEquals("orders", event.getSubject());
        assertEquals("application/json", event.getDataContentType());
        assertNotNull(event.getTime());
        assertNotNull(event.getData());
        assertEquals("tenant-1", event.getExtension("xtenantid"));
        assertEquals("bk-1", event.getExtension("xbusinesskey"));
        assertEquals("42", event.getExtension("xoffset"));
        assertNotNull(event.getExtension("xheaders"));
    }

    @Test
    void buildCloudEventOmitsAbsentFields() {
        var record = new ConsumerRecord()
                .setOffset(7L)
                .setId("id-2")
                .setType("t.ping")
                .setTopic("orders");

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertEquals("id-2", event.getId());
        assertNull(event.getTime());
        assertNull(event.getData());
        assertNull(event.getExtension("xtenantid"));
        assertNull(event.getExtension("xbusinesskey"));
        assertNull(event.getExtension("xheaders"));
        assertEquals("7", event.getExtension("xoffset"));
    }

    @Test
    void buildCloudEventOmitsBlankStringsAndEmptyHeaders() {
        var record = new ConsumerRecord()
                .setOffset(1L)
                .setId("id-3")
                .setType("t.ping")
                .setTopic("orders")
                .setTenantId(" ")
                .setBusinessKey("")
                .setHeaders(Map.of());

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertNull(event.getExtension("xtenantid"));
        assertNull(event.getExtension("xbusinesskey"));
        assertNull(event.getExtension("xheaders"));
    }

}
