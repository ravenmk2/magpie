package ravenworks.magpie.engine.impl.rabbitmq;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.PartitionUtils;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.OffsetTracker;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.testsupport.TestRabbitMq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * RabbitMQ Stream 边界 IT：真实 broker 上验证 create/send/poll 往返与
 * offset 提交语义（未提交重启重投、已提交不重投——重启恢复的兜底机制）。
 *
 * <p>每个测试方法用独立 topic/stream 隔离（consumer 从 offset -1 即从头开始，
 * 共享 stream 会读到其他用例的残留消息）；stream 名带随机后缀，容器随测试 JVM
 * 退出被 Ryuk 回收，无需显式清理。
 *
 * <p>OffsetTracker 用内存 fake（本 IT 不覆盖 DB 边界，DB 侧由
 * LeaderLockRepositoryIT 等负责）。
 */
class RabbitStreamIT {

    private static final int PARTITIONS = 2;

    /**
     * 内存 OffsetTracker：read 缺省返回 -1（从头消费），与 OffsetTrackerImpl 的语义一致。
     */
    private static final Map<String, Long> OFFSETS = new ConcurrentHashMap<>();
    private static final OffsetTracker OFFSET_TRACKER = new OffsetTracker() {
        @Override
        public long read(String name, int partition) {
            return OFFSETS.getOrDefault(name + ":" + partition, -1L);
        }

        @Override
        public void write(String name, int partition, long offset) {
            OFFSETS.put(name + ":" + partition, offset);
        }
    };

    private static RabbitStreamProvider provider;

    @BeforeAll
    static void setUp() {
        provider = new RabbitStreamProvider(List.of(TestRabbitMq.streamUri()), OFFSET_TRACKER);
    }

    @AfterAll
    static void tearDown() {
        provider.close();
    }

    @Test
    void sendAndPollRoundTrip() throws Exception {
        String topic = newTopic("roundtrip");
        try (StreamProducer producer = newProducer(topic)) {
            List<StreamConsumer> consumers = provider.consumer(definition(topic), "it-roundtrip");
            consumers.forEach(StreamConsumer::start);
            try {
                Set<String> sent = Set.of("alpha", "bravo", "charlie");
                sent.forEach(key -> send(producer, topic, key, "payload-" + key));

                List<ConsumerRecord> received = pollUntil(consumers, sent.size());
                assertEquals(sent, received.stream()
                        .map(r -> r.getMessage().getBusinessKey()).collect(Collectors.toSet()));
                for (ConsumerRecord record : received) {
                    MessageRecord message = record.getMessage();
                    assertEquals("payload-" + message.getBusinessKey(),
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                    assertEquals("it-type", message.getType());
                    assertEquals("it-tenant", message.getTenantId());
                    assertEquals(topic, message.getTopic());
                    assertEquals("it-value", message.getHeaders().get("it-header"));
                }
            } finally {
                consumers.forEach(StreamConsumer::stop);
            }
        }
    }

    @Test
    void committedOffsetSurvivesConsumerRestart() throws Exception {
        String topic = newTopic("commit");
        String name = "it-commit";
        String key = "same-key";
        int total = 3;

        try (StreamProducer producer = newProducer(topic)) {
            for (int i = 0; i < total; i++) {
                send(producer, topic, key, "seq-" + i);
            }
        }

        StreamDefinition definition = definition(topic);

        // 阶段一：全部消费但不提交，重启后应原样重投（未提交 offset 的兜底）
        List<StreamConsumer> first = provider.consumer(definition, name);
        first.forEach(StreamConsumer::start);
        List<ConsumerRecord> firstBatch = pollUntil(first, total);
        first.forEach(StreamConsumer::stop);

        List<StreamConsumer> second = provider.consumer(definition, name);
        second.forEach(StreamConsumer::start);
        List<ConsumerRecord> redelivered = pollUntil(second, total);
        assertEquals(
                firstBatch.stream().map(r -> r.getMessage().getId()).collect(Collectors.toSet()),
                redelivered.stream().map(r -> r.getMessage().getId()).collect(Collectors.toSet()));

        // 阶段二：提交最大 offset 后重启，不应再有重投
        // 同 key 消息全部落在同一分区，commit 走该分区的 consumer
        long maxOffset = redelivered.stream().mapToLong(ConsumerRecord::getOffset).max().orElseThrow();
        second.get(PartitionUtils.partition(key, PARTITIONS)).commit(maxOffset);
        second.forEach(StreamConsumer::stop);

        List<StreamConsumer> third = provider.consumer(definition, name);
        third.forEach(StreamConsumer::start);
        try {
            // 持续轮询 3s，断言没有任何重投（等待缺席比 Awaitility during 更直观）
            List<ConsumerRecord> afterCommit = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (System.nanoTime() < deadline) {
                afterCommit.addAll(pollOnce(third));
            }
            assertTrue(afterCommit.isEmpty(), "committed offset must not be redelivered");
        } finally {
            third.forEach(StreamConsumer::stop);
        }
    }

    private static String newTopic(String prefix) {
        return "it-" + prefix + "-" + Uuids.uuidHex().substring(0, 12);
    }

    private static StreamDefinition definition(String topic) {
        return new StreamDefinition(topic, PARTITIONS, Map.of());
    }

    private static StreamProducer newProducer(String topic) {
        StreamDefinition definition = definition(topic);
        provider.create(definition);
        return provider.producer(definition);
    }

    private static void send(StreamProducer producer, String topic, String businessKey, String payload) {
        var record = new MessageRecord()
                .setId(Uuids.uuidHex())
                .setType("it-type")
                .setEventTime(LocalDateTime.now())
                .setTopic(topic)
                .setTenantId("it-tenant")
                .setBusinessKey(businessKey)
                .setHeaders(Map.of("it-header", "it-value"))
                .setPayload(payload.getBytes(StandardCharsets.UTF_8));
        var result = producer.send(record).join();
        assertTrue(result.isSucceeded(), "send failed: " + result.getError());
    }

    /**
     * 轮询所有分区直到收满 expected 条（Awaitility 超时 30s）。
     */
    private static List<ConsumerRecord> pollUntil(List<StreamConsumer> consumers, int expected) {
        List<ConsumerRecord> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).until(() -> {
            received.addAll(pollOnce(consumers));
            return received.size() >= expected;
        });
        return received;
    }

    private static List<ConsumerRecord> pollOnce(List<StreamConsumer> consumers) {
        List<ConsumerRecord> batch = new ArrayList<>();
        for (StreamConsumer consumer : consumers) {
            batch.addAll(consumer.poll(10, Duration.ofMillis(200)));
        }
        return batch;
    }

}
