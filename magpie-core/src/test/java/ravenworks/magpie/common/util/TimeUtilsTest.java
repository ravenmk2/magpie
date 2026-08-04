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

}
