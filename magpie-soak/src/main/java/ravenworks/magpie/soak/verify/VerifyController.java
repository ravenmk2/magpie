package ravenworks.magpie.soak.verify;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ravenworks.magpie.soak.probe.ProbeFactory;
import ravenworks.magpie.soak.probe.ProbeMessage;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * 投递校验端点：http sink 的投递目标。一切输入问题（未知通道、非探针消息、
 * business key 与载荷不符）只记 invalid 指标、一律返回 204——返回错误码会让
 * ORDERED 通道原地重试一条永远无法成功的消息，毒化整条流；裁判的职责是观测，
 * 不是改变被测系统的行为。
 */
@Slf4j
@RestController
public class VerifyController {

    private static final EventFormat JSON_FORMAT =
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);

    private final Map<String, SequenceTracker> trackers;
    private final MeterRegistry metrics;
    private final Map<String, Timer> latencyTimers;
    private final Map<String, Counter> invalidCounters;

    public VerifyController(Map<String, SequenceTracker> trackers, MeterRegistry metrics) {
        this.trackers = trackers;
        this.metrics = metrics;
        this.latencyTimers = new LinkedHashMap<>();
        this.invalidCounters = new LinkedHashMap<>();
        trackers.keySet().forEach(channel -> {
            this.latencyTimers.put(channel,
                    Timer.builder("soak.e2e.latency")
                            .description("probe end-to-end latency")
                            .tag("channel", channel)
                            .publishPercentileHistogram(true)
                            .register(metrics));
            // 同样启动即注册：零值序列让"无无效探针"与"没采集到"可区分
            this.invalidCounters.put(channel, metrics.counter("soak.invalid", "channel", channel));
        });
    }

    @PostMapping("/events/{channel}")
    public ResponseEntity<Void> receive(@PathVariable String channel, @RequestBody byte[] body) {
        SequenceTracker tracker = this.trackers.get(channel);
        if (tracker == null) {
            invalid(channel, "unknown channel");
            return ResponseEntity.noContent().build();
        }
        try {
            CloudEvent event = JSON_FORMAT.deserialize(body);
            if (event.getData() == null) {
                invalid(channel, "event without data");
                return ResponseEntity.noContent().build();
            }
            ProbeMessage probe = ProbeFactory.parse(event.getData().toBytes());
            if (probe.key() == null || probe.key().isBlank()) {
                invalid(channel, "probe without key");
                return ResponseEntity.noContent().build();
            }
            Object businessKey = event.getExtension(ProbeFactory.EXT_BUSINESS_KEY);
            if (businessKey == null || !businessKey.toString().equals(probe.key())) {
                invalid(channel, "xbusinesskey mismatch");
                return ResponseEntity.noContent().build();
            }
            long now = System.currentTimeMillis();
            tracker.onProbe(probe.key(), probe.seq(), now);
            this.latencyTimers.get(channel).record(Duration.ofMillis(Math.max(0, now - probe.sentAt())));
        } catch (Exception e) {
            invalid(channel, e.getMessage());
        }
        return ResponseEntity.noContent().build();
    }

    private void invalid(String channel, String reason) {
        var counter = this.invalidCounters.get(channel);
        if (counter != null) {
            counter.increment();
        } else {
            this.metrics.counter("soak.invalid", "channel", channel).increment();
        }
        log.warn("invalid probe on channel '{}': {}", channel, reason);
    }

}
