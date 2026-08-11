package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.MessageHandler.Context;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.TimeUtils;
import ravenworks.magpie.engine.api.stream.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Raven
 */
@Slf4j
public class RabbitStreamConsumer implements StreamConsumer {

    private static final Set<String> KNOWN_KEYS = ReservedHeaders.HEADERS;

    private final Environment environment;
    private final StreamDefinition definition;
    private final int partition;
    private final String topic;
    private final String name;
    private final OffsetTracker offsetTracker;
    private final AtomicBoolean consuming = new AtomicBoolean(false);

    private volatile Consumer rmqConsumer;
    private volatile BlockingQueue<QueuedItem> queue;

    public RabbitStreamConsumer(@NonNull Environment environment,
                                @NonNull StreamDefinition definition,
                                int partition,
                                @NonNull String name,
                                @NonNull OffsetTracker offsetTracker) {
        this.environment = environment;
        this.definition = definition;
        this.partition = partition;
        this.name = name;
        this.offsetTracker = offsetTracker;
        this.topic = definition.name();
    }

    @Override
    public int partition() {
        return this.partition;
    }

    @Override
    public void start() {
        if (!this.consuming.compareAndSet(false, true)) {
            throw new IllegalStateException("Already started");
        }
        this.queue = new LinkedBlockingQueue<>();

        String streamName = RabbitUtils.streamQueueName(this.definition.name(), this.partition);
        this.rmqConsumer = this.environment.consumerBuilder()
                .stream(streamName)
                .name("magpie-" + this.name + "-" + this.partition)
                // Single Active Consumer：分组键为 (stream, name)，同名 consumer 同组，
                // broker 保证同时只有一个 active，active 消失自动接管（offset 按 name 跟踪，天然兼容）
                .singleActiveConsumer()
                .flow()
                .strategy(ConsumerFlowStrategy.creditOnProcessedMessageCount(10, 0.5))
                .builder()
                .manualTrackingStrategy()
                .builder()
                .subscriptionListener(ctx -> ctx.offsetSpecification(trackedOffset()))
                // SAC 激活（首次激活与接管）时 broker 会再询问一次起始 offset（consumer update）。
                // 默认实现只查服务端存储的 offset，本项目不走服务端跟踪，会回退到 next()
                // 跳过全部存量消息；这里与 subscriptionListener 保持一致，统一以 OffsetTracker 为准
                .consumerUpdateListener(ctx -> ctx.isActive() ? trackedOffset() : null)
                .messageHandler((ctx, msg) -> {
                    try {
                        this.queue.put(new QueuedItem(ctx, msg));
                    } catch (InterruptedException e) {
                        log.info("[{}] partition={} thread interrupted", this.name, this.partition);
                        Thread.currentThread().interrupt();
                    }
                })
                .build();
        log.info("[{}] partition={} consumer started", this.name, this.partition);
    }

    @Override
    public List<ConsumerRecord> poll(int count, @NonNull Duration timeout) {
        if (this.queue == null) {
            return List.of();
        }
        List<ConsumerRecord> result = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        for (int i = 0; i < count; i++) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }
            QueuedItem item;
            try {
                item = this.queue.poll(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                log.info("[{}] partition={} poll interrupted", this.name, this.partition);
                Thread.currentThread().interrupt();
                break;
            }
            if (item == null) {
                break;
            }
            item.ctx().processed();
            ConsumerRecord record = this.convert(item.ctx(), item.msg());
            result.add(record);
        }
        return result;
    }

    @Override
    public void commit(long offset) {
        try {
            this.offsetTracker.write(this.name, this.partition, offset + 1);
        } catch (Exception e) {
            log.warn("[{}] partition={} failed to commit offset={}",
                    this.name, this.partition, offset, e);
        }
    }

    @Override
    public void stop() {
        if (this.rmqConsumer != null) {
            this.rmqConsumer.close();
            this.rmqConsumer = null;
        }
        if (this.queue != null) {
            this.queue.clear();
            this.queue = null;
        }
        this.consuming.set(false);
    }

    /**
     * 以 OffsetTracker（DB）为准的起始位置：订阅建立与 SAC 激活时都从这里恢复。
     */
    private OffsetSpecification trackedOffset() {
        long offset = this.offsetTracker.read(this.name, this.partition);
        log.info("[{}] partition={} resuming from offset={}", this.name, this.partition, offset);
        return resolveOffset(offset);
    }

    private static OffsetSpecification resolveOffset(long offset) {
        if (offset < -1) {
            return OffsetSpecification.next();
        }
        if (offset == -1) {
            return OffsetSpecification.first();
        }
        return OffsetSpecification.offset(offset);
    }

    private ConsumerRecord convert(@NonNull Context ctx, @NonNull Message msg) {
        var message = new MessageRecord();
        message.setPayload(msg.getBodyAsBinary());
        message.setTopic(this.topic);

        if (msg.getProperties() != null) {
            Object mid = msg.getProperties().getMessageId();
            if (mid != null) {
                message.setId(mid.toString());
            }
        }
        Map<String, Object> appProps = msg.getApplicationProperties();
        if (appProps != null) {
            message.setType((String) appProps.get(ReservedHeaders.MSG_TYPE));
            message.setTenantId((String) appProps.get(ReservedHeaders.MSG_TENANT_ID));
            message.setBusinessKey((String) appProps.get(ReservedHeaders.MSG_BUSINESS_KEY));
            String timeStr = (String) appProps.get(ReservedHeaders.MSG_EVENT_TIME);
            if (timeStr != null) {
                message.setEventTime(TimeUtils.parseRfc3339(timeStr));
            }
            Map<String, String> headers = new HashMap<>();
            appProps.forEach((k, v) -> {
                if (!KNOWN_KEYS.contains(k) && v instanceof String s) {
                    headers.put(k, s);
                }
            });
            message.setHeaders(headers);
        }
        return new ConsumerRecord()
                .setOffset(ctx.offset())
                .setMessage(message);
    }

    private record QueuedItem(Context ctx, Message msg) {

    }

}
