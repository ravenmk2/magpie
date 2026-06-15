package ravenworks.magpie.engine.source.mysql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public interface SendStrategy {

    List<List<OutboxRecord>> partition(List<OutboxRecord> records);


    final class OrderedStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(List<OutboxRecord> records) {
            List<List<OutboxRecord>> batches = new ArrayList<>(records.size());
            for (var record : records) {
                batches.add(List.of(record));
            }
            return batches;
        }

    }


    final class KeyOrderedStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(List<OutboxRecord> records) {
            List<List<OutboxRecord>> batches = new ArrayList<>();
            List<OutboxRecord> current = new ArrayList<>();
            Set<String> seenKeys = new HashSet<>();

            for (var record : records) {
                String key = resolveKey(record);
                if (seenKeys.contains(key)) {
                    batches.add(List.copyOf(current));
                    current.clear();
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

        private static String resolveKey(OutboxRecord record) {
            return record.getBusinessKey() != null && !record.getBusinessKey().isEmpty()
                    ? record.getBusinessKey()
                    : record.getId();
        }

    }


    final class BestEffortStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(List<OutboxRecord> records) {
            return List.of(List.copyOf(records));
        }

    }

}
