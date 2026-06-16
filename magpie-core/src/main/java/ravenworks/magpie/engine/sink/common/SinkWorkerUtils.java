package ravenworks.magpie.engine.sink.common;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.util.List;


@Slf4j
@UtilityClass
class SinkWorkerUtils {

    static List<ConsumerRecord> filterByOffset(@NonNull String name,
                                               @NonNull List<ConsumerRecord> batch,
                                               long lastOffset) {
        if (lastOffset < 0) {
            return batch;
        }
        int before = batch.size();
        var filtered = batch.stream()
                .filter(r -> r.getOffset() > lastOffset)
                .toList();
        int skipped = before - filtered.size();
        if (skipped > 0) {
            log.warn("[{}] skipped {} already-processed messages (offset <= {})",
                    name, skipped, lastOffset);
        }
        return filtered;
    }

}
