package ravenworks.magpie.engine.sink.common;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkWorkerUtilsTest {

    private static ConsumerRecord record(long offset) {
        return new ConsumerRecord().setOffset(offset);
    }

    @Test
    void negativeLastOffsetReturnsBatchUnchanged() {
        var batch = List.of(record(1), record(2));
        assertSame(batch, SinkWorkerUtils.filterByOffset("t", batch, -1));
    }

    @Test
    void filtersAlreadyProcessedOffsets() {
        var batch = List.of(record(1), record(2), record(3), record(4));
        var filtered = SinkWorkerUtils.filterByOffset("t", batch, 2);
        assertEquals(2, filtered.size());
        assertEquals(3, filtered.get(0).getOffset());
        assertEquals(4, filtered.get(1).getOffset());
    }

    @Test
    void recordAtLastOffsetIsFilteredOut() {
        var batch = List.of(record(3), record(4));
        var filtered = SinkWorkerUtils.filterByOffset("t", batch, 3);
        assertEquals(1, filtered.size());
        assertEquals(4, filtered.get(0).getOffset());
    }

    @Test
    void allFilteredReturnsEmpty() {
        var batch = List.of(record(1), record(2));
        assertTrue(SinkWorkerUtils.filterByOffset("t", batch, 5).isEmpty());
    }

}
