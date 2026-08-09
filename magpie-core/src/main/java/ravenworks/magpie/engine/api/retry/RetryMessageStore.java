package ravenworks.magpie.engine.api.retry;

import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;


/**
 * 重试消息存储。读取契约：{@link #list} 按 consumer 内 offset 升序全量返回；
 * {@link #listRetryable} 额外按 retryAt 到期过滤。调用方分工：
 * KEY_ORDERED 用 list——同 Key 顺序由"offset 升序读取 + 按序投递 + 失败中断"保证，
 * 退避由调用方的内存时点节制，不依赖 retryAt；BEST_EFFORT 用 listRetryable——
 * 无顺序要求，直接依赖 retryAt 字段（重启后仍生效）。
 */
public interface RetryMessageStore {

    Set<String> listKeys(String consumer);

    List<RetryRecord> list(String consumer, int count);

    List<RetryRecord> listRetryable(String consumer, int count);

    void save(String consumer, ConsumerRecord record);

    void succeeded(String id);

    /**
     * 记录一次失败：attempts 自增。可重试时间由调用方按退避策略计算后传入。
     */
    void failed(String id, LocalDateTime retryAt);

}
