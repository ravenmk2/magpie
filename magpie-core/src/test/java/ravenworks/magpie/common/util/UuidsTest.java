package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class UuidsTest {

    @Test
    void uuidHexIs32LowerHexChars() {
        String hex = Uuids.uuidHex();
        assertEquals(32, hex.length());
        assertTrue(hex.matches("[0-9a-f]{32}"), "unexpected format: " + hex);
    }

    @Test
    void uuidHexIsUnique() {
        var seen = new HashSet<String>();
        for (int i = 0; i < 1_000; i++) {
            assertTrue(seen.add(Uuids.uuidHex()));
        }
    }

    @Test
    void toHexMatchesUuidToStringWithoutDashes() {
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid.toString().replace("-", ""), Uuids.toHex(uuid));
    }

    @Test
    void uuid7HasVersion7() {
        assertEquals(7, Uuids.uuid7().version());
    }

    @Test
    void uuid7HexIs32CharsWithVersionMarker() {
        String hex = Uuids.uuid7Hex();
        assertEquals(32, hex.length());
        assertTrue(hex.matches("[0-9a-f]{32}"), "unexpected format: " + hex);
        assertEquals('7', hex.charAt(12));
    }

}
