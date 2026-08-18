package ravenworks.magpie.testkit.verify;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.LinkedHashMap;
import java.util.Map;


@Configuration
@Profile("verifier")
@EnableScheduling
@EnableConfigurationProperties(VerifyProperties.class)
public class VerifyConfig {

    /**
     * 每通道一个 SequenceTracker，判定事件落到带通道标签的 Prometheus 计数器。
     * 计数器启动即注册（恒有 0 值序列）：看板与告警据此区分
     * "零违规"（贴地直线）与"没采集到数据"（序列缺失）。
     */
    @Bean
    public Map<String, SequenceTracker> trackers(VerifyProperties props, MeterRegistry metrics) {
        var trackers = new LinkedHashMap<String, SequenceTracker>();
        props.getChannels().forEach((channel, semantics) -> {
            var listener = new ChannelListener(channel, metrics);
            var tracker = new SequenceTracker(semantics,
                    props.getGapTimeout().toMillis(), props.getMaxPendingPerKey(),
                    props.getStaleAfter().toMillis(), listener);
            metrics.gauge("testkit.active_keys", Tags.of("channel", channel),
                    tracker, t -> t.snapshot().activeKeys());
            trackers.put(channel, tracker);
        });
        return trackers;
    }

    @Bean
    public VerifyController verifyController(Map<String, SequenceTracker> trackers, MeterRegistry metrics) {
        return new VerifyController(trackers, metrics);
    }

    @Bean
    public ReportLogger reportLogger(Map<String, SequenceTracker> trackers) {
        return new ReportLogger(trackers);
    }

    private static final class ChannelListener implements SequenceTracker.Listener {

        private final Counter received;
        private final Counter duplicates;
        private final Counter outOfOrder;
        private final Counter lost;

        private ChannelListener(String channel, MeterRegistry metrics) {
            this.received = metrics.counter("testkit.received", "channel", channel);
            this.duplicates = metrics.counter("testkit.duplicates", "channel", channel);
            this.outOfOrder = metrics.counter("testkit.out_of_order", "channel", channel);
            this.lost = metrics.counter("testkit.lost", "channel", channel);
        }

        @Override
        public void onReceived() {
            this.received.increment();
        }

        @Override
        public void onDuplicate() {
            this.duplicates.increment();
        }

        @Override
        public void onOutOfOrder() {
            this.outOfOrder.increment();
        }

        @Override
        public void onLost(long count) {
            this.lost.increment(count);
        }

    }

}
