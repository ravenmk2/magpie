package ravenworks.magpie.common.json;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

}
