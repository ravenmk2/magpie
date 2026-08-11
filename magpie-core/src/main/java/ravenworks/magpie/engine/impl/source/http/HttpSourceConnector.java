package ravenworks.magpie.engine.impl.source.http;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.cloudevents.CloudEvent;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.http.*;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;


/**
 * @author Raven
 */
@Slf4j
public class HttpSourceConnector implements SourceConnector {

    private static final String EXT_TENANT_ID = "xtenantid";
    private static final String EXT_BUSINESS_KEY = "xbusinesskey";
    private static final int MAX_CACHE_SIZE = 10_000;
    /**
     * message_id 约定：32 字符 uuid7 hex
     */
    private static final Pattern UUID_HEX_32 = Pattern.compile("[0-9a-fA-F]{32}");
    /**
     * 字段长度上限：message_id ≤32（magpie_message_log.message_id 为 CHAR(32)），
     * type/topic/tenant_id/business_key ≤256（VARCHAR(256)）。绝不截断，超长入口即拒。
     */
    private static final int MAX_MESSAGE_ID_LENGTH = 32;
    private static final int MAX_FIELD_LENGTH = 256;

    private final HttpSourceRouter router;
    private final StreamProducer producer;
    private final String name;
    private final Set<String> exactTopics;
    private final List<Pattern> wildcardPatterns;
    private final Cache<String, Boolean> wildcardCache;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public HttpSourceConnector(@NonNull HttpSourceRouter router,
                               @NonNull StreamProducer producer,
                               @NonNull String name,
                               @NonNull Map<String, Object> properties) {
        this.router = router;
        this.producer = producer;
        this.name = name;
        var config = new HttpSourceProperties();
        PropertiesUtils.bind(config, properties);

        var exact = new HashSet<String>();
        var wildcards = new ArrayList<Pattern>();
        for (String pattern : config.getAllowedTopics()) {
            if (pattern.indexOf('*') < 0) {
                exact.add(pattern);
            } else {
                wildcards.add(compileGlob(pattern));
            }
        }
        this.exactTopics = Set.copyOf(exact);
        this.wildcardPatterns = List.copyOf(wildcards);
        this.wildcardCache = Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        this.router.subscribe(this.name, this::onMessage);
        this.running.set(true);
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        this.running.set(false);
        this.router.unsubscribe(this.name);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isAlive() {
        return this.running.get();
    }

    private void onMessage(HttpMessageContext context) {
        var result = context.result();
        var event = context.event();
        String topic = event.getSubject();
        try {
            validateFieldLengths(event, topic);
        } catch (InvalidMessageException e) {
            result.completeExceptionally(e);
            return;
        }
        if (topic == null || topic.isBlank() || !this.isAllowed(topic)) {
            result.completeExceptionally(new TopicNotAllowedException(topic));
            return;
        }
        try {
            var record = this.toMessageRecord(event, topic);
            this.producer.send(record).whenComplete((sendResult, error) -> {
                if (error != null) {
                    result.completeExceptionally(new PublishFailedException(error.getMessage(), error));
                } else if (sendResult == null || !sendResult.isSucceeded()) {
                    String reason = sendResult != null ? sendResult.getError() : "unknown error";
                    result.completeExceptionally(new PublishFailedException(reason));
                } else {
                    result.complete(null);
                }
            });
        } catch (Throwable e) {
            result.completeExceptionally(new PublishFailedException(e.getMessage(), e));
        }
    }

    private MessageRecord toMessageRecord(CloudEvent event, String topic) {
        String id = event.getId();
        if (id == null || !UUID_HEX_32.matcher(id).matches()) {
            // 不合规的客户端 id 一律替换为新生成的 uuid7，保证全链路 message_id 恒为 32 字符
            id = Uuids.uuid7Hex();
        }

        LocalDateTime eventTime = event.getTime() != null
                ? LocalDateTime.ofInstant(event.getTime().toInstant(), ZoneId.systemDefault())
                : LocalDateTime.now();

        byte[] payload = event.getData() != null ? event.getData().toBytes() : new byte[0];

        Map<String, String> headers = new LinkedHashMap<>();
        for (String ext : event.getExtensionNames()) {
            if (EXT_TENANT_ID.equals(ext) || EXT_BUSINESS_KEY.equals(ext)) {
                continue;
            }
            Object value = event.getExtension(ext);
            if (value != null) {
                headers.put(ext, String.valueOf(value));
            }
        }

        return new MessageRecord()
                .setId(id)
                .setType(event.getType())
                .setEventTime(eventTime)
                .setTopic(topic)
                .setTenantId(extension(event, EXT_TENANT_ID))
                .setBusinessKey(extension(event, EXT_BUSINESS_KEY))
                .setHeaders(headers)
                .setPayload(payload);
    }

    private static String extension(CloudEvent event, String name) {
        Object value = event.getExtension(name);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * 入口边界校验：字段超长是错误而非截断理由（截断会破坏业务关联），
     * 在进入 stream 之前直接拒绝。null 视为 ""，允许缺省。
     */
    private static void validateFieldLengths(CloudEvent event, String topic) {
        requireLength("id", event.getId(), MAX_MESSAGE_ID_LENGTH);
        requireLength("type", event.getType(), MAX_FIELD_LENGTH);
        requireLength("topic", topic, MAX_FIELD_LENGTH);
        requireLength("xtenantid", extension(event, EXT_TENANT_ID), MAX_FIELD_LENGTH);
        requireLength("xbusinesskey", extension(event, EXT_BUSINESS_KEY), MAX_FIELD_LENGTH);
    }

    private static void requireLength(String field, String value, int maxLength) {
        if (value != null && value.length() > maxLength) {
            throw new InvalidMessageException(field, value.length(), maxLength);
        }
    }

    private boolean isAllowed(String topic) {
        if (this.exactTopics.contains(topic)) {
            return true;
        }
        if (this.wildcardPatterns.isEmpty()) {
            return false;
        }
        return this.wildcardCache.get(topic, this::matchesAnyWildcard);
    }

    private boolean matchesAnyWildcard(String topic) {
        for (Pattern pattern : this.wildcardPatterns) {
            if (pattern.matcher(topic).matches()) {
                return true;
            }
        }
        return false;
    }

    private static Pattern compileGlob(String pattern) {
        String[] parts = pattern.split("\\*", -1);
        var regex = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                regex.append(".*");
            }
            if (!parts[i].isEmpty()) {
                regex.append(Pattern.quote(parts[i]));
            }
        }
        return Pattern.compile(regex.toString());
    }

}
