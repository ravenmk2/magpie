package ravenworks.magpie.engine.retry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryMessageStoreImplTest {

    @Test
    void firstRetryDelayIsBaseDelay() {
        assertEquals(5_000, RetryMessageStoreImpl.computeRetryDelayMs(1));
    }

    @Test
    void retryDelayDoublesPerAttempt() {
        assertEquals(10_000, RetryMessageStoreImpl.computeRetryDelayMs(2));
        assertEquals(20_000, RetryMessageStoreImpl.computeRetryDelayMs(3));
        assertEquals(40_000, RetryMessageStoreImpl.computeRetryDelayMs(4));
    }

    @Test
    void retryDelayIsCappedAtMax() {
        assertEquals(300_000, RetryMessageStoreImpl.computeRetryDelayMs(7));
        assertEquals(300_000, RetryMessageStoreImpl.computeRetryDelayMs(100));
        assertEquals(300_000, RetryMessageStoreImpl.computeRetryDelayMs(Integer.MAX_VALUE));
    }

    @Test
    void retryDelayNeverOverflows() {
        assertTrue(RetryMessageStoreImpl.computeRetryDelayMs(0) > 0);
        assertTrue(RetryMessageStoreImpl.computeRetryDelayMs(-1) > 0);
    }

    @Test
    void nullMessageIdGeneratesUuid7() {
        var id = RetryMessageStoreImpl.normalizeMessageId(null);
        assertTrue(id.matches("[0-9a-f]{32}"), "expected 32-char hex, got: " + id);
        assertNotEquals(id, RetryMessageStoreImpl.normalizeMessageId(null));
    }

    @Test
    void blankMessageIdGeneratesUuid7() {
        assertTrue(RetryMessageStoreImpl.normalizeMessageId("  ").matches("[0-9a-f]{32}"));
    }

    @Test
    void conformingMessageIdIsKept() {
        assertEquals("0123456789abcdef0123456789abcdef",
                RetryMessageStoreImpl.normalizeMessageId("0123456789abcdef0123456789abcdef"));
    }

    @Test
    void longMessageIdIsTruncatedTo32() {
        var id = RetryMessageStoreImpl.normalizeMessageId("x".repeat(40));
        assertEquals(32, id.length());
        assertEquals("x".repeat(32), id);
    }

    @Test
    void nullToEmptyCoalescesNull() {
        assertEquals("", RetryMessageStoreImpl.nullToEmpty(null));
        assertEquals("v", RetryMessageStoreImpl.nullToEmpty("v"));
    }

}
