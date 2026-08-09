package ravenworks.magpie.domain.converter;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class JsonMapConverterTest {

    private final JsonMapConverter converter = new JsonMapConverter();

    @Test
    void nullMapsToNullBothWays() {
        assertNull(this.converter.convertToDatabaseColumn(null));
        assertNull(this.converter.convertToEntityAttribute(null));
    }

    @Test
    void roundTripPreservesValues() {
        Map<String, Object> attribute = Map.of("s", "v", "n", 1, "b", true);
        String column = this.converter.convertToDatabaseColumn(attribute);
        assertEquals(attribute, this.converter.convertToEntityAttribute(column));
    }

    @Test
    void roundTripPreservesNestedAndUnicode() {
        Map<String, Object> attribute = Map.of(
                "nested", Map.of("a", 1),
                "text", "消息总线");
        String column = this.converter.convertToDatabaseColumn(attribute);
        assertEquals(attribute, this.converter.convertToEntityAttribute(column));
    }

}
