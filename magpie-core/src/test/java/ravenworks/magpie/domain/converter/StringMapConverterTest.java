package ravenworks.magpie.domain.converter;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class StringMapConverterTest {

    private final StringMapConverter converter = new StringMapConverter();

    @Test
    void nullMapsToNullBothWays() {
        assertNull(this.converter.convertToDatabaseColumn(null));
        assertNull(this.converter.convertToEntityAttribute(null));
    }

    @Test
    void roundTripPreservesValues() {
        Map<String, String> attribute = Map.of("k1", "v1", "k2", "v2");
        String column = this.converter.convertToDatabaseColumn(attribute);
        assertEquals(attribute, this.converter.convertToEntityAttribute(column));
    }

    @Test
    void roundTripPreservesUnicode() {
        Map<String, String> attribute = Map.of("key", "消息总线");
        String column = this.converter.convertToDatabaseColumn(attribute);
        assertEquals(attribute, this.converter.convertToEntityAttribute(column));
    }

}
