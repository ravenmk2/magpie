package ravenworks.magpie.engine.sink.http;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkStatus;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
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

    private static HttpSinkHandler newHandler(String url, int maxAttempts) {
        var config = HttpSinkHandlerConfig.of(Map.of("url", url));
        config.setDelayMs(0);
        config.setMaxAttempts(maxAttempts);
        return new HttpSinkHandler("t", HttpClient.newHttpClient(),
                new CircuitBreaker("t", 100, 1, 1_000), config);
    }

    @Test
    void unserializableRecordFailsOnlyItselfWithoutCircuitBreaker() {
        var handler = newHandler("http://127.0.0.1:1/x", 1);
        try {
            // id 缺失, CloudEvent 构建必失败: 不经 HTTP(attempts=0), 仅本条 FAILURE
            var record = new ConsumerRecord()
                    .setOffset(0)
                    .setType("t.test")
                    .setTopic("topic");
            var result = handler.handle(record).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(0, result.getAttempts());
            assertNotNull(result.getError());
            assertEquals(record, result.getRecord());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void invalidUrlIsRetriedLikeSystemicFailure() {
        var handler = newHandler("://invalid-url", 2);
        try {
            var record = new ConsumerRecord()
                    .setOffset(1)
                    .setId("id-x")
                    .setType("t.test")
                    .setTopic("topic");
            // IllegalArgumentException 与 IO 错误同属系统性故障: 退避重试直到 maxAttempts
            var result = handler.handle(record).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(2, result.getAttempts());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void batchIsolatesPoisonedAndFailingRecords() {
        var handler = newHandler("http://127.0.0.1:1/x", 1);
        try {
            var poisoned = new ConsumerRecord()
                    .setOffset(0)
                    .setType("t.test")
                    .setTopic("topic"); // id 缺失, 序列化失败
            var unreachable = new ConsumerRecord()
                    .setOffset(1)
                    .setId("id-ok")
                    .setType("t.test")
                    .setTopic("topic"); // 端点连不通, 系统性失败
            // join 不抛异常: 各条的失败被隔离为各自的 FAILURE, 不连坐整批
            var results = handler.handle(List.of(poisoned, unreachable)).join();
            assertEquals(2, results.size());
            assertEquals(SinkStatus.FAILURE, results.get(0).getStatus());
            assertEquals(0, results.get(0).getAttempts());
            assertEquals(SinkStatus.FAILURE, results.get(1).getStatus());
            assertEquals(1, results.get(1).getAttempts());
        } finally {
            handler.shutdown().join();
        }
    }

}
