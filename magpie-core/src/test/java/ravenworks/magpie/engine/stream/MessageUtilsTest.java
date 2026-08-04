package ravenworks.magpie.engine.stream;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageUtilsTest {

    private static List<List<String>> batch(String... keys) {
        return MessageUtils.batchByUniqueKey(List.of(keys), Function.identity());
    }

    @Test
    void emptyInputProducesNoBatches() {
        assertEquals(List.of(), MessageUtils.batchByUniqueKey(List.of(), Function.identity()));
    }

    @Test
    void allUniqueKeysFormSingleBatch() {
        assertEquals(List.of(List.of("a", "b", "c")), batch("a", "b", "c"));
    }

    @Test
    void duplicateKeySplitsBatch() {
        assertEquals(List.of(List.of("a", "b"), List.of("a")), batch("a", "b", "a"));
    }

    @Test
    void consecutiveDuplicateKeysSplitEveryTime() {
        assertEquals(List.of(List.of("a"), List.of("a")), batch("a", "a"));
    }

    @Test
    void mixedPatternSplitsOnRepeatOfSeenKey() {
        assertEquals(List.of(List.of("a"), List.of("a", "b"), List.of("b")), batch("a", "a", "b", "b"));
    }

    @Test
    void keyExtractorDeterminesUniqueness() {
        // keys: a, a, b, c, a -> split on repeat of a seen key
        var batches = MessageUtils.batchByUniqueKey(
                List.of("a1", "a2", "b1", "c1", "a3"), s -> s.substring(0, 1));
        assertEquals(List.of(List.of("a1"), List.of("a2", "b1", "c1"), List.of("a3")), batches);
    }

}
