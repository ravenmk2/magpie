package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class PartitionUtilsTest {

    @Test
    void emptyKeyRoutesToPartitionZero() {
        assertEquals(0, PartitionUtils.partition("", 8));
    }

    @Test
    void nullKeyRoutesToPartitionZero() {
        // null 归一为空串：无 businessKey 的消息视为同一条队列，不抛 NPE
        assertEquals(0, PartitionUtils.partition(null, 8));
    }

    @Test
    void sameKeyAlwaysRoutesToSamePartition() {
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            assertEquals(PartitionUtils.partition(key, 8), PartitionUtils.partition(key, 8));
        }
    }

    @Test
    void partitionIsWithinRange() {
        for (int i = 0; i < 1_000; i++) {
            int p = PartitionUtils.partition("key-" + i, 8);
            assertTrue(p >= 0 && p < 8, "partition out of range: " + p);
        }
    }

    @Test
    void singlePartitionAlwaysZero() {
        for (int i = 0; i < 100; i++) {
            assertEquals(0, PartitionUtils.partition("key-" + i, 1));
        }
    }

    @Test
    void zeroPartitionsRejected() {
        // partitions <= 0 没有取模意义：参数校验抛出 IllegalArgumentException，而非 ArithmeticException
        assertThrows(IllegalArgumentException.class, () -> PartitionUtils.partition("key", 0));
    }

    @Test
    void negativePartitionsRejected() {
        assertThrows(IllegalArgumentException.class, () -> PartitionUtils.partition("key", -3));
    }

}
