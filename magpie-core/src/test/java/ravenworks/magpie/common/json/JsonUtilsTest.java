package ravenworks.magpie.common.json;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class JsonUtilsTest {

    record Point(int x, int y) {

    }

    @Test
    void encodeDecodeRoundTrip() {
        var point = new Point(1, 2);
        String json = JsonUtils.encode(point);
        assertEquals("{\"x\":1,\"y\":2}", json);
        assertEquals(point, JsonUtils.decode(json, Point.class));
    }

    @Test
    void decodeWithTypeReference() {
        Map<String, Integer> map = JsonUtils.decode("{\"a\":1,\"b\":2}", new TypeReference<>() {

        });
        assertEquals(Map.of("a", 1, "b", 2), map);
    }

    @Test
    void decodeInvalidJsonThrows() {
        assertThrows(JsonException.class, () -> JsonUtils.decode("{bad json", Point.class));
    }

    public static class Exploding {

        public String getBoom() {
            throw new IllegalStateException("boom");
        }

    }

    @Test
    void encodeFailureThrows() {
        assertThrows(JsonException.class, () -> JsonUtils.encode(new Exploding()));
    }

    record Event(String name, LocalDateTime at) {

    }

    @Test
    void decodeIgnoresUnknownFields() {
        // FAIL_ON_UNKNOWN_PROPERTIES=false：多余字段直接忽略
        var point = JsonUtils.decode("{\"x\":1,\"y\":2,\"z\":3}", Point.class);
        assertEquals(new Point(1, 2), point);
    }

    @Test
    void localDateTimeRoundTrip() {
        // JavaTimeModule + WRITE_DATES_AS_TIMESTAMPS=false：ISO-8601 文本往返
        var event = new Event("e", LocalDateTime.of(2026, 8, 4, 12, 30, 45));
        String json = JsonUtils.encode(event);
        assertEquals("{\"name\":\"e\",\"at\":\"2026-08-04T12:30:45\"}", json);
        assertEquals(event, JsonUtils.decode(json, Event.class));
    }

    @Test
    void decodeEmptyStringThrows() {
        // 空输入不是合法 JSON 文档
        assertThrows(JsonException.class, () -> JsonUtils.decode("", Point.class));
    }

    @Test
    void decodeJsonNullReturnsNull() {
        assertNull(JsonUtils.decode("null", Point.class));
    }

    @Test
    void encodeNullReturnsJsonNull() {
        assertEquals("null", JsonUtils.encode(null));
    }

}
