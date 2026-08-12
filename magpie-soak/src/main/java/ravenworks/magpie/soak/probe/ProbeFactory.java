package ravenworks.magpie.soak.probe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Arrays;


/**
 * 探针构造：序列化 ProbeMessage，并包装成发布端点接受的 structured CloudEvent。
 * loadgen 走 HTTP 发布用 {@link #cloudEvent}，outbox 播种只需要 {@link #payload}。
 */
public final class ProbeFactory {

    public static final String EVENT_TYPE = "soak.probe";
    public static final String EXT_BUSINESS_KEY = "xbusinesskey";

    private static final URI EVENT_SOURCE = URI.create("magpie-soak");
    private static final EventFormat JSON_FORMAT =
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final NoArgGenerator UUID_V7 = Generators.timeBasedEpochGenerator();

    private ProbeFactory() {
    }

    /**
     * 消息 ID：与 Magpie 全链路约定一致的 uuid7 hex（32 字符，无连字符）
     */
    public static String newId() {
        return UUID_V7.generate().toString().replace("-", "");
    }

    public static ProbeMessage probe(String key, long seq, int payloadSize) {
        return new ProbeMessage(key, seq, System.currentTimeMillis(), padOf(payloadSize));
    }

    public static byte[] payload(String key, long seq, int payloadSize) {
        try {
            return MAPPER.writeValueAsBytes(probe(key, seq, payloadSize));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize probe", e);
        }
    }

    public static byte[] cloudEvent(String topic, String key, long seq, int payloadSize) {
        CloudEvent event = CloudEventBuilder.v1()
                .withId(newId())
                .withSource(EVENT_SOURCE)
                .withType(EVENT_TYPE)
                .withSubject(topic)
                .withTime(OffsetDateTime.now())
                .withDataContentType("application/json")
                .withExtension(EXT_BUSINESS_KEY, key)
                .withData(payload(key, seq, payloadSize))
                .build();
        return JSON_FORMAT.serialize(event);
    }

    public static ProbeMessage parse(byte[] payload) {
        try {
            return MAPPER.readValue(payload, ProbeMessage.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Not a probe payload: " + e.getMessage(), e);
        }
    }

    private static String padOf(int payloadSize) {
        // 估算固定字段开销后补齐，非精确尺寸，仅用于制造目标载荷量级
        int overhead = 96;
        int length = Math.max(0, payloadSize - overhead);
        var pad = new char[length];
        Arrays.fill(pad, 'x');
        return new String(pad);
    }

}
