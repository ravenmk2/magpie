package ravenworks.magpie.engine.impl.source.mysql;

import lombok.NonNull;
import ravenworks.magpie.engine.api.stream.MessageUtils;

import java.util.List;
import java.util.Locale;


/**
 * 发送策略：把按 (created_at, id) 升序读出的批次切成有序子批。
 * 执行（子批内并行、子批间串行、失败处置）由 connector 统一完成，
 * 策略只决定分组方式与单条失败后的处置动作。
 *
 * @author Raven
 */
public interface SendStrategy {

    /**
     * 单条消息发送失败后的处置动作（仅本轮有效，失败行留在表内下轮重投）。
     */
    enum FailurePolicy {
        /**
         * 队头阻塞：停止本轮发送
         */
        STOP,
        /**
         * 跳过本轮内同 businessKey 的后续消息，其他 Key 不受影响
         */
        SKIP_KEY,
        /**
         * 不阻塞，继续发送其余消息
         */
        CONTINUE
    }

    /**
     * 把批次切成有序子批：子批间串行、子批内并行。
     */
    List<List<OutboxRecord>> partition(@NonNull List<OutboxRecord> batch);

    FailurePolicy failurePolicy();

    /**
     * 缺省或非法值回落 best_effort。
     */
    static SendStrategy of(String name) {
        var normalized = name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "ordered" -> new OrderedStrategy();
            case "key_ordered" -> new KeyOrderedStrategy();
            default -> new BestEffortStrategy();
        };
    }

    /**
     * businessKey 归一化：null 视为空串（与 NOT NULL 列约定一致）。
     */
    static String keyOf(@NonNull OutboxRecord record) {
        var key = record.getBusinessKey();
        return key == null ? "" : key;
    }


    /**
     * 严格按表内顺序：每行一组，逐条发送，失败即队头阻塞。
     */
    class OrderedStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(@NonNull List<OutboxRecord> batch) {
            return batch.stream().<List<OutboxRecord>>map(List::of).toList();
        }

        @Override
        public FailurePolicy failurePolicy() {
            return FailurePolicy.STOP;
        }

    }


    /**
     * 同 businessKey 按顺序、不同 Key 可并行：按 Key 切连续子批（同 Key 必落不同子批），
     * 某 Key 失败后本轮跳过该 Key 的后续消息。
     */
    class KeyOrderedStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(@NonNull List<OutboxRecord> batch) {
            return MessageUtils.batchByUniqueKey(batch, SendStrategy::keyOf);
        }

        @Override
        public FailurePolicy failurePolicy() {
            return FailurePolicy.SKIP_KEY;
        }

    }


    /**
     * 尽量按顺序但允许乱序：整批并发发送，个别失败不阻塞其他消息。
     */
    class BestEffortStrategy implements SendStrategy {

        @Override
        public List<List<OutboxRecord>> partition(@NonNull List<OutboxRecord> batch) {
            return List.of(batch);
        }

        @Override
        public FailurePolicy failurePolicy() {
            return FailurePolicy.CONTINUE;
        }

    }

}
