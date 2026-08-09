package ravenworks.magpie.engine.impl.source.mysql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;


class SendStrategyTest {

    private static OutboxRecord record(String id, String businessKey) {
        var r = new OutboxRecord();
        r.setId(id);
        r.setBusinessKey(businessKey);
        return r;
    }

    private static List<String> ids(List<OutboxRecord> group) {
        return group.stream().map(OutboxRecord::getId).toList();
    }

    @Test
    void orderedPartitionsOneRowPerGroup() {
        var batch = List.of(record("a", "k"), record("b", "k"), record("c", "k"));

        var groups = new SendStrategy.OrderedStrategy().partition(batch);

        assertEquals(3, groups.size());
        groups.forEach(g -> assertEquals(1, g.size()));
        assertEquals(List.of("a", "b", "c"), groups.stream().flatMap(List::stream).map(OutboxRecord::getId).toList());
        assertEquals(SendStrategy.FailurePolicy.STOP, new SendStrategy.OrderedStrategy().failurePolicy());
    }

    @Test
    void keyOrderedKeepsSameKeyInDifferentGroupsAndPreservesGlobalOrder() {
        var batch = List.of(
                record("a1", "A"), record("b1", "B"), record("a2", "A"),
                record("c1", "C"), record("a3", "A"));

        var groups = new SendStrategy.KeyOrderedStrategy().partition(batch);

        assertEquals(List.of("a1", "b1"), ids(groups.get(0)));
        assertEquals(List.of("a2", "c1"), ids(groups.get(1)));
        assertEquals(List.of("a3"), ids(groups.get(2)));
        // 展开后与输入顺序一致
        assertEquals(List.of("a1", "b1", "a2", "c1", "a3"),
                groups.stream().flatMap(List::stream).map(OutboxRecord::getId).toList());
        assertEquals(SendStrategy.FailurePolicy.SKIP_KEY, new SendStrategy.KeyOrderedStrategy().failurePolicy());
    }

    @Test
    void keyOrderedNormalizesNullKeyToEmpty() {
        assertEquals("", SendStrategy.keyOf(record("x", null)));
        var groups = new SendStrategy.KeyOrderedStrategy()
                .partition(List.of(record("a", null), record("b", ""), record("c", null)));
        assertEquals(3, groups.size());
    }

    @Test
    void bestEffortPartitionsWholeBatchAsOneGroup() {
        var batch = List.of(record("a", "A"), record("b", "A"), record("c", "B"));

        var groups = new SendStrategy.BestEffortStrategy().partition(batch);

        assertEquals(1, groups.size());
        assertEquals(List.of("a", "b", "c"), ids(groups.get(0)));
        assertEquals(SendStrategy.FailurePolicy.CONTINUE, new SendStrategy.BestEffortStrategy().failurePolicy());
    }

    @Test
    void factoryFallsBackToBestEffort() {
        assertInstanceOf(SendStrategy.OrderedStrategy.class, SendStrategy.of("ordered"));
        assertInstanceOf(SendStrategy.KeyOrderedStrategy.class, SendStrategy.of("key_ordered"));
        assertInstanceOf(SendStrategy.BestEffortStrategy.class, SendStrategy.of("best_effort"));
        assertInstanceOf(SendStrategy.BestEffortStrategy.class, SendStrategy.of("garbage"));
        assertInstanceOf(SendStrategy.BestEffortStrategy.class, SendStrategy.of(null));
    }

    @Test
    void factoryNormalizesCaseAndWhitespace() {
        // of() 归一化：trim + 转小写，大小写与首尾空白不敏感
        assertInstanceOf(SendStrategy.OrderedStrategy.class, SendStrategy.of("ORDERED"));
        assertInstanceOf(SendStrategy.OrderedStrategy.class, SendStrategy.of("  Ordered "));
        assertInstanceOf(SendStrategy.KeyOrderedStrategy.class, SendStrategy.of("Key_Ordered"));
        assertInstanceOf(SendStrategy.KeyOrderedStrategy.class, SendStrategy.of("\tKEY_ORDERED\n"));
        assertInstanceOf(SendStrategy.BestEffortStrategy.class, SendStrategy.of("BEST_EFFORT"));
    }

}
