package ravenworks.magpie.engine.api.stream;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


@UtilityClass
public class MessageUtils {

    public static <T> List<List<T>> batchByUniqueKey(@NonNull List<T> records,
                                                     @NonNull Function<T, String> keyExtractor) {
        List<List<T>> batches = new ArrayList<>();
        List<T> current = new ArrayList<>();
        Set<String> seenKeys = new HashSet<>();
        for (var record : records) {
            String key = keyExtractor.apply(record);
            if (seenKeys.contains(key)) {
                batches.add(List.copyOf(current));
                current = new ArrayList<>();
                seenKeys.clear();
            }
            seenKeys.add(key);
            current.add(record);
        }
        if (!current.isEmpty()) {
            batches.add(List.copyOf(current));
        }
        return batches;
    }

}
