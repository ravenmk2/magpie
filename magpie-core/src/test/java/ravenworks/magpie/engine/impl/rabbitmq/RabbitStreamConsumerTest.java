package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.ConsumerUpdateListener;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamStats;
import com.rabbitmq.stream.SubscriptionListener;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.OffsetTracker;
import ravenworks.magpie.engine.api.stream.ReservedHeaders;
import ravenworks.magpie.engine.api.stream.StreamDefinition;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * RabbitStreamConsumer 的 convert / resolveOffset 纯逻辑测试。
 * 两者均为私有方法，通过手写 stub 模拟 Environment 的 consumerBuilder 链，
 * 捕获 subscriptionListener 与 messageHandler 回调，无需真实 broker。
 */
class RabbitStreamConsumerTest {

    /** 测试夹具：持有捕获到的回调与可脚本化的 offset。 */
    private static final class Fixture {
        final AtomicLong trackedOffset = new AtomicLong(-1);
        final AtomicReference<MessageHandler> messageHandler = new AtomicReference<>();
        final AtomicReference<SubscriptionListener> subscriptionListener = new AtomicReference<>();
        final RabbitStreamConsumer consumer;

        Fixture() {
            OffsetTracker tracker = new OffsetTracker() {
                @Override
                public long read(String name, int partition) {
                    return Fixture.this.trackedOffset.get();
                }

                @Override
                public void write(String name, int partition, long offset) {
                    // 测试不涉及 commit
                }
            };
            this.consumer = new RabbitStreamConsumer(
                    new FakeEnvironment(this.messageHandler, this.subscriptionListener),
                    new StreamDefinition("topic-a", 1, null),
                    0,
                    "test-consumer",
                    tracker);
            this.consumer.start();
        }

        /** 将一条消息经 messageHandler 送入队列，再通过 poll 触发 convert。 */
        ConsumerRecord deliver(MessageHandler.Context ctx, Message msg) {
            this.messageHandler.get().handle(ctx, msg);
            List<ConsumerRecord> records = this.consumer.poll(1, Duration.ofSeconds(1));
            assertEquals(1, records.size());
            return records.get(0);
        }

        /** 触发 subscriptionListener，返回最终写入的 OffsetSpecification。 */
        OffsetSpecification resolveOffset() {
            var specRef = new AtomicReference<OffsetSpecification>();
            this.subscriptionListener.get().preSubscribe(new SubscriptionListener.SubscriptionContext() {
                @Override
                public OffsetSpecification offsetSpecification() {
                    return specRef.get();
                }

                @Override
                public void offsetSpecification(OffsetSpecification spec) {
                    specRef.set(spec);
                }

                @Override
                public String stream() {
                    return "test-stream";
                }
            });
            return specRef.get();
        }
    }

    // ---------- resolveOffset（经 subscriptionListener 间接验证） ----------

    @Test
    void resolveOffsetMinusOneResumesFromFirst() {
        var fixture = new Fixture();
        fixture.trackedOffset.set(-1);
        assertEquals(OffsetSpecification.first(), fixture.resolveOffset());
    }

    @Test
    void resolveOffsetBelowMinusOneResumesFromNext() {
        var fixture = new Fixture();
        fixture.trackedOffset.set(-2);
        assertEquals(OffsetSpecification.next(), fixture.resolveOffset());
        fixture.trackedOffset.set(-100);
        assertEquals(OffsetSpecification.next(), fixture.resolveOffset());
    }

    @Test
    void resolveOffsetNonNegativeResumesFromExactOffset() {
        var fixture = new Fixture();
        fixture.trackedOffset.set(0);
        assertEquals(OffsetSpecification.offset(0), fixture.resolveOffset());
        fixture.trackedOffset.set(42);
        assertEquals(OffsetSpecification.offset(42), fixture.resolveOffset());
    }

    // ---------- convert（经 messageHandler + poll 间接验证） ----------

    @Test
    void convertPropagatesPayloadTopicAndOffset() {
        var fixture = new Fixture();
        byte[] payload = "hello".getBytes();
        var record = fixture.deliver(new FakeContext(123L), new FakeMessage(payload, null, null));
        assertEquals(123L, record.getOffset());
        assertArrayEquals(payload, record.getMessage().getPayload());
        assertEquals("topic-a", record.getMessage().getTopic());
    }

    @Test
    void convertKeepsIdNullWhenMessageIdMissing() {
        var fixture = new Fixture();
        // properties 为 null：id 不生成、不兜底，保持 null
        var record = fixture.deliver(new FakeContext(0L), new FakeMessage(new byte[0], null, null));
        assertNull(record.getMessage().getId());

        // properties 存在但 messageId 为 null：同样保持 null
        var record2 = fixture.deliver(new FakeContext(1L),
                new FakeMessage(new byte[0], new FakeProperties(null), null));
        assertNull(record2.getMessage().getId());
    }

    @Test
    void convertUsesMessageIdToString() {
        var fixture = new Fixture();
        var record = fixture.deliver(new FakeContext(0L),
                new FakeMessage(new byte[0], new FakeProperties("msg-001"), null));
        assertEquals("msg-001", record.getMessage().getId());

        // 非 String 类型的 messageId 走 toString
        var record2 = fixture.deliver(new FakeContext(1L),
                new FakeMessage(new byte[0], new FakeProperties(98765L), null));
        assertEquals("98765", record2.getMessage().getId());
    }

    @Test
    void convertLeavesEventTimeNullWhenHeaderMissing() {
        var fixture = new Fixture();
        var record = fixture.deliver(new FakeContext(0L),
                new FakeMessage(new byte[0], null, Map.of("k", "v")));
        assertNull(record.getMessage().getEventTime());
    }

    @Test
    void convertParsesEventTimeHeader() {
        var fixture = new Fixture();
        var appProps = Map.<String, Object>of(
                ReservedHeaders.MSG_EVENT_TIME, "2026-08-08T12:34:56+08:00");
        var record = fixture.deliver(new FakeContext(0L),
                new FakeMessage(new byte[0], null, appProps));
        assertEquals(LocalDateTime.of(2026, 8, 8, 12, 34, 56), record.getMessage().getEventTime());
    }

    @Test
    void convertThrowsOnInvalidEventTimeHeader() {
        var fixture = new Fixture();
        // TimeUtils.parseRfc3339 无兜底，非法值直接抛 DateTimeParseException
        var appProps = Map.<String, Object>of(ReservedHeaders.MSG_EVENT_TIME, "not-a-time");
        var msg = new FakeMessage(new byte[0], null, appProps);
        assertThrows(DateTimeParseException.class,
                () -> fixture.deliver(new FakeContext(0L), msg));
    }

    @Test
    void convertMapsReservedApplicationProperties() {
        var fixture = new Fixture();
        var appProps = Map.<String, Object>of(
                ReservedHeaders.MSG_TYPE, "t.order",
                ReservedHeaders.MSG_TENANT_ID, "tenant-1",
                ReservedHeaders.MSG_BUSINESS_KEY, "biz-9");
        var record = fixture.deliver(new FakeContext(0L),
                new FakeMessage(new byte[0], null, appProps));
        var message = record.getMessage();
        assertEquals("t.order", message.getType());
        assertEquals("tenant-1", message.getTenantId());
        assertEquals("biz-9", message.getBusinessKey());
        // 保留头不进入 headers
        assertTrue(message.getHeaders().isEmpty());
    }

    @Test
    void convertFiltersHeaders() {
        var fixture = new Fixture();
        var appProps = new HashMap<String, Object>();
        appProps.put("x-custom", "yes");                       // 非保留 String：透传
        appProps.put("x-number", 42);                          // 非 String：丢弃
        appProps.put("x-uuid", UUID.randomUUID());             // 非 String：丢弃
        appProps.put(ReservedHeaders.MSG_TYPE, "t.x");         // 保留头：排除
        appProps.put(ReservedHeaders.MSG_TENANT_ID, "t1");     // 保留头：排除
        var record = fixture.deliver(new FakeContext(0L),
                new FakeMessage(new byte[0], null, appProps));
        assertEquals(Map.of("x-custom", "yes"), record.getMessage().getHeaders());
    }

    @Test
    void convertLeavesHeadersNullWhenNoApplicationProperties() {
        var fixture = new Fixture();
        var record = fixture.deliver(new FakeContext(0L), new FakeMessage(new byte[0], null, null));
        assertNull(record.getMessage().getHeaders());
        assertNull(record.getMessage().getType());
        assertNull(record.getMessage().getTenantId());
        assertNull(record.getMessage().getBusinessKey());
    }

    // ---------- 手写 stub ----------

    /** 可控 offset/timestamp 的 MessageHandler.Context。 */
    private static final class FakeContext implements MessageHandler.Context {
        private final long offset;

        FakeContext(long offset) {
            this.offset = offset;
        }

        @Override
        public long offset() {
            return this.offset;
        }

        @Override
        public void storeOffset() {
        }

        @Override
        public long timestamp() {
            return 1723000000000L;
        }

        @Override
        public long committedChunkId() {
            return 0;
        }

        @Override
        public String stream() {
            return "test-stream";
        }

        @Override
        public Consumer consumer() {
            return null;
        }

        @Override
        public void processed() {
        }
    }

    /** 可控 body/properties/applicationProperties 的 Message。 */
    private static final class FakeMessage implements Message {
        private final byte[] body;
        private final Properties properties;
        private final Map<String, Object> applicationProperties;

        FakeMessage(byte[] body, Properties properties, Map<String, Object> applicationProperties) {
            this.body = body;
            this.properties = properties;
            this.applicationProperties = applicationProperties;
        }

        @Override
        public boolean hasPublishingId() {
            return false;
        }

        @Override
        public long getPublishingId() {
            return 0;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return this.body;
        }

        @Override
        public Object getBody() {
            return this.body;
        }

        @Override
        public Properties getProperties() {
            return this.properties;
        }

        @Override
        public Map<String, Object> getApplicationProperties() {
            return this.applicationProperties;
        }

        @Override
        public Map<String, Object> getMessageAnnotations() {
            return null;
        }
    }

    /** 仅 messageId 可控的 Properties，其余返回默认值。 */
    private static final class FakeProperties implements Properties {
        private final Object messageId;

        FakeProperties(Object messageId) {
            this.messageId = messageId;
        }

        @Override
        public Object getMessageId() {
            return this.messageId;
        }

        @Override
        public String getMessageIdAsString() {
            return null;
        }

        @Override
        public long getMessageIdAsLong() {
            return 0;
        }

        @Override
        public byte[] getMessageIdAsBinary() {
            return null;
        }

        @Override
        public UUID getMessageIdAsUuid() {
            return null;
        }

        @Override
        public byte[] getUserId() {
            return null;
        }

        @Override
        public String getTo() {
            return null;
        }

        @Override
        public String getSubject() {
            return null;
        }

        @Override
        public String getReplyTo() {
            return null;
        }

        @Override
        public Object getCorrelationId() {
            return null;
        }

        @Override
        public String getCorrelationIdAsString() {
            return null;
        }

        @Override
        public long getCorrelationIdAsLong() {
            return 0;
        }

        @Override
        public byte[] getCorrelationIdAsBinary() {
            return null;
        }

        @Override
        public UUID getCorrelationIdAsUuid() {
            return null;
        }

        @Override
        public String getContentType() {
            return null;
        }

        @Override
        public String getContentEncoding() {
            return null;
        }

        @Override
        public long getAbsoluteExpiryTime() {
            return 0;
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public String getGroupId() {
            return null;
        }

        @Override
        public long getGroupSequence() {
            return 0;
        }

        @Override
        public String getReplyToGroupId() {
            return null;
        }
    }

    /** 捕获回调的 ConsumerBuilder 链 stub。 */
    private static final class FakeConsumerBuilder implements ConsumerBuilder {
        private final AtomicReference<MessageHandler> messageHandler;
        private final AtomicReference<SubscriptionListener> subscriptionListener;

        FakeConsumerBuilder(AtomicReference<MessageHandler> messageHandler,
                            AtomicReference<SubscriptionListener> subscriptionListener) {
            this.messageHandler = messageHandler;
            this.subscriptionListener = subscriptionListener;
        }

        @Override
        public ConsumerBuilder stream(String stream) {
            return this;
        }

        @Override
        public ConsumerBuilder superStream(String superStream) {
            return this;
        }

        @Override
        public ConsumerBuilder offset(OffsetSpecification offsetSpecification) {
            return this;
        }

        @Override
        public ConsumerBuilder messageHandler(MessageHandler handler) {
            this.messageHandler.set(handler);
            return this;
        }

        @Override
        public ConsumerBuilder name(String name) {
            return this;
        }

        @Override
        public ConsumerBuilder singleActiveConsumer() {
            return this;
        }

        @Override
        public ConsumerBuilder consumerUpdateListener(ConsumerUpdateListener listener) {
            return this;
        }

        @Override
        public ConsumerBuilder subscriptionListener(SubscriptionListener listener) {
            this.subscriptionListener.set(listener);
            return this;
        }

        @Override
        public ConsumerBuilder listeners(Resource.StateListener... listeners) {
            return this;
        }

        @Override
        public ManualTrackingStrategy manualTrackingStrategy() {
            return new ManualTrackingStrategy() {
                @Override
                public ManualTrackingStrategy checkInterval(Duration duration) {
                    return this;
                }

                @Override
                public ConsumerBuilder builder() {
                    return FakeConsumerBuilder.this;
                }
            };
        }

        @Override
        public AutoTrackingStrategy autoTrackingStrategy() {
            return new AutoTrackingStrategy() {
                @Override
                public AutoTrackingStrategy messageCountBeforeStorage(int count) {
                    return this;
                }

                @Override
                public AutoTrackingStrategy flushInterval(Duration interval) {
                    return this;
                }

                @Override
                public ConsumerBuilder builder() {
                    return FakeConsumerBuilder.this;
                }
            };
        }

        @Override
        public ConsumerBuilder noTrackingStrategy() {
            return this;
        }

        @Override
        public FilterConfiguration filter() {
            return new FilterConfiguration() {
                @Override
                public FilterConfiguration values(String... values) {
                    return this;
                }

                @Override
                public FilterConfiguration postFilter(Predicate<Message> predicate) {
                    return this;
                }

                @Override
                public FilterConfiguration matchUnfiltered() {
                    return this;
                }

                @Override
                public FilterConfiguration matchUnfiltered(boolean matchUnfiltered) {
                    return this;
                }

                @Override
                public ConsumerBuilder builder() {
                    return FakeConsumerBuilder.this;
                }
            };
        }

        @Override
        public FlowConfiguration flow() {
            return new FlowConfiguration() {
                @Override
                public FlowConfiguration initialCredits(int initialCredits) {
                    return this;
                }

                @Override
                public FlowConfiguration strategy(ConsumerFlowStrategy strategy) {
                    return this;
                }

                @Override
                public ConsumerBuilder builder() {
                    return FakeConsumerBuilder.this;
                }
            };
        }

        @Override
        public Consumer build() {
            return new Consumer() {
                @Override
                public void store(long offset) {
                }

                @Override
                public void close() {
                }

                @Override
                public long storedOffset() {
                    return -1;
                }
            };
        }
    }

    /** 仅 consumerBuilder 可用的 Environment stub。 */
    private static final class FakeEnvironment implements Environment {
        private final FakeConsumerBuilder consumerBuilder;

        FakeEnvironment(AtomicReference<MessageHandler> messageHandler,
                        AtomicReference<SubscriptionListener> subscriptionListener) {
            this.consumerBuilder = new FakeConsumerBuilder(messageHandler, subscriptionListener);
        }

        @Override
        public StreamCreator streamCreator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteStream(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSuperStream(String superStream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamStats queryStreamStats(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeOffset(String reference, String stream, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean streamExists(String stream) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProducerBuilder producerBuilder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConsumerBuilder consumerBuilder() {
            return this.consumerBuilder;
        }

        @Override
        public void close() {
        }
    }

}
