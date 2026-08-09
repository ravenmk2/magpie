package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class TimeUtilsTest {

    @Test
    void formatFollowsRfc3339WithOffset() {
        String formatted = TimeUtils.formatRfc3339(LocalDateTime.of(2026, 8, 4, 12, 30, 45));
        assertTrue(formatted.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{2}:\\d{2})"),
                "unexpected format: " + formatted);
    }

    @Test
    void formatParseRoundTrip() {
        var time = LocalDateTime.of(2026, 8, 4, 12, 30, 45);
        assertEquals(time, TimeUtils.parseRfc3339(TimeUtils.formatRfc3339(time)));
    }

    @Test
    void parseKeepsWallTimeOfNonSystemOffset() {
        // 钉住实际行为：parseRfc3339 直接保留输入偏移量下的墙面时间，不按偏移换算到系统时区。
        // 注意与 formatRfc3339 不对称：format 用系统时区附加偏移，parse 却不换算，
        // 因此仅当输入偏移与系统时区一致时二者才互为往返。
        assertEquals(LocalDateTime.of(2026, 8, 4, 12, 30, 45),
                TimeUtils.parseRfc3339("2026-08-04T12:30:45+09:00"));
    }

}
