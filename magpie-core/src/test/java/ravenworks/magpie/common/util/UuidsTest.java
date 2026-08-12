package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


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

    @Test
    void uuid7HexTimestampPrefixIsNonDecreasing() {
        // 可排序性来自前 12 个 hex 字符的毫秒时间戳：前缀保证非递减
        String prevPrefix = "";
        for (int i = 0; i < 1_000; i++) {
            String hex = Uuids.uuid7Hex();
            String prefix = hex.substring(0, 12);
            assertTrue(prefix.compareTo(prevPrefix) >= 0,
                    "timestamp prefix went backwards: " + prevPrefix + " -> " + prefix);
            prevPrefix = prefix;
        }
    }

    @Test
    void uuid7HexIsStrictlyIncreasingWithinSameMillisecond() {
        // outbox 按 (created_at, id) 排序的决胜键：同一毫秒内的 uuid7 也必须字典序递增
        // （timeBasedEpochGenerator 的计数器变体，区别于随机后缀变体）
        List<String> batch = new ArrayList<>();
        String prefix = null;
        for (int i = 0; i < 10_000; i++) {
            String hex = Uuids.uuid7Hex();
            String p = hex.substring(0, 12);
            if (!p.equals(prefix)) {
                if (batch.size() >= 2) {
                    break;
                }
                prefix = p;
                batch.clear();
            }
            batch.add(hex);
        }
        assertTrue(batch.size() >= 2, "no same-millisecond batch found");

        for (int i = 1; i < batch.size(); i++) {
            assertTrue(batch.get(i).compareTo(batch.get(i - 1)) > 0,
                    "same-millisecond batch not strictly increasing at index " + i + ": " + batch);
        }
    }

}
