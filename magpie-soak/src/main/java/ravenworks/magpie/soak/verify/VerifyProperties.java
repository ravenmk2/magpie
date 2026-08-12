package ravenworks.magpie.soak.verify;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;


@Data
@ConfigurationProperties("soak.verify")
public class VerifyProperties {

    /**
     * 校验通道：通道名 → 语义（STRICT 同 key 首次 seq 必须连续；RELAXED 允许乱序，只判丢失）。
     * 与 deploy/soak 中 target 的 url 路径一一对应。
     */
    private Map<String, SequenceTracker.Semantics> channels = new LinkedHashMap<>(Map.of(
            "ordered", SequenceTracker.Semantics.STRICT,
            "key-ordered", SequenceTracker.Semantics.STRICT,
            "best-effort", SequenceTracker.Semantics.RELAXED,
            "outbox", SequenceTracker.Semantics.RELAXED));

    /**
     * RELAXED 通道缺口判丢失的宽限：必须覆盖重试退避上限（5min）与重启重投窗口
     */
    private Duration gapTimeout = Duration.ofMinutes(15);

    /**
     * 每 key pending 缓冲上限，超出按丢失收口（防异常 key 撑爆内存）
     */
    private int maxPendingPerKey = 10_000;

    /**
     * key 不活跃多久后驱逐出状态表
     */
    private Duration staleAfter = Duration.ofHours(24);

}
