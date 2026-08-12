package ravenworks.magpie.soak.verify;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;


/**
 * 周期维护与汇报：每 30s 巡检一次（结算超时缺口、驱逐不活跃 key），
 * 每 5min 输出一行文本汇总，作为指标之外的离线可读记录。
 */
@Slf4j
public class ReportLogger {

    private final Map<String, SequenceTracker> trackers;

    public ReportLogger(Map<String, SequenceTracker> trackers) {
        this.trackers = trackers;
    }

    @Scheduled(fixedDelay = 30_000)
    public void sweep() {
        long now = System.currentTimeMillis();
        this.trackers.values().forEach(t -> t.sweep(now));
    }

    @Scheduled(fixedDelay = 300_000)
    public void report() {
        this.trackers.forEach((channel, tracker) -> {
            var s = tracker.snapshot();
            log.info("soak report [{}] received={} duplicates={} outOfOrder={} lost={} activeKeys={}",
                    channel, s.received(), s.duplicates(), s.outOfOrder(), s.lost(), s.activeKeys());
        });
    }

}
