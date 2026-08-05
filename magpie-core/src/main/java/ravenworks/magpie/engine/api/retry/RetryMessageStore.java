package ravenworks.magpie.engine.api.retry;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.Set;


/**
 * 重试消息存储。不变式：同 consumer 内相同 businessKey 的消息按 offset 顺序重试——
 * save/failed 会把更晚同 key 条目的 retryAt 推到不早于更老条目，调用方只需按
 * （retryAt 到期, offset 升序）读取即可保证 key 内顺序。
 */
public interface RetryMessageStore {

    Set<String> listKeys(String consumer);

    List<RetryRecord> list(String consumer, int count);

    List<RetryRecord> listRetryable(String consumer, int count);

    void save(String consumer, ConsumerRecord record);

    void succeeded(String id);

    /**
     * 记录一次失败：attempts 自增，并按指数退避计算下次可重试时间（无次数上限，无 DLQ）。
     */
    void failed(String id);

}
