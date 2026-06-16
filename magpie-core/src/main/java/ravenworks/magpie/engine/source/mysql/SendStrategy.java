package ravenworks.magpie.engine.source.mysql;

import ravenworks.magpie.engine.stream.MessageUtils;

import java.util.ArrayList;
import java.util.List;


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
            return MessageUtils.batchByUniqueKey(records, KeyOrderedStrategy::resolveKey);
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
