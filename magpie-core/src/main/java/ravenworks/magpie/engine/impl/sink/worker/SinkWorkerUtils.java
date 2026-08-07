package ravenworks.magpie.engine.impl.sink.worker;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopState;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.util.List;
import java.util.concurrent.locks.LockSupport;


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

    /**
     * 落库失败原地重试：未落库的消息不能推进 offset，否则提交后重启也无法找回。
     * 关闭中不再重试，直接抛出（offset 未提交，等重启重投）。
     */
    static void saveWithRetry(@NonNull RetryMessageStore store,
                              @NonNull String name,
                              @NonNull ConsumerRecord record,
                              @NonNull WorkLoop workLoop) {
        while (true) {
            try {
                store.save(name, record);
                return;
            } catch (RuntimeException e) {
                if (workLoop.getState() != WorkLoopState.RUNNING) {
                    throw e;
                }
                log.warn("[{}] save retry message failed (offset={}), retry in 1s: {}",
                        name, record.getOffset(), e.toString());
                LockSupport.parkNanos(1_000_000_000L);
            }
        }
    }

}
