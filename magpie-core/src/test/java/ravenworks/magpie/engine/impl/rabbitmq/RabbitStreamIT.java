package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamException;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.PartitionUtils;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.api.stream.*;
import ravenworks.magpie.testsupport.TestRabbitMq;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


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
        provider = new RabbitStreamProvider(TestRabbitMq.streamOptions(), OFFSET_TRACKER);
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

    /**
     * flow credit 补给边界：初始 credit 只有 10（creditOnProcessedMessageCount(10, 0.5)），
     * 后续全靠 poll 里的 ctx.processed() 回调补给。发 50 条（远超初始 credit）小批量慢速消费，
     * 若 processed() 被误删/错位，consumer 会在 credit 耗尽后停滞，Awaitility 超时暴露。
     */
    @Test
    void creditReplenishmentDeliversBeyondInitialCredit() throws Exception {
        String topic = newTopic("credit");
        StreamDefinition definition = new StreamDefinition(topic, 1, Map.of());
        int total = 50;

        provider.create(definition);
        try (StreamProducer producer = provider.producer(definition)) {
            for (int i = 0; i < total; i++) {
                send(producer, topic, "key-" + i, "seq-" + i);
            }

            List<StreamConsumer> consumers = provider.consumer(definition, "it-credit");
            consumers.forEach(StreamConsumer::start);
            try {
                // 每次只 poll 5 条（小于初始 credit 10），慢速小批量消费。超时时长放宽：
                // credit 按条细粒度回补，远程共享 Docker 主机上投递速率受负载影响；
                // 若 processed() 被误删则彻底停摆（永远只收到初始 10 条），放宽不影响钉停滞
                List<ConsumerRecord> received = new ArrayList<>();
                try {
                    await().atMost(Duration.ofSeconds(90)).until(() -> {
                        received.addAll(consumers.get(0).poll(5, Duration.ofMillis(500)));
                        return received.size() >= total;
                    });
                } catch (ConditionTimeoutException e) {
                    fail("consumer stalled at " + received.size() + "/" + total
                            + " messages (flow credit not replenished?)", e);
                }

                // 单分区：offset 必须连续 0..49
                List<Long> offsets = received.stream()
                        .map(ConsumerRecord::getOffset).sorted().toList();
                assertEquals(total, offsets.size());
                for (int i = 0; i < total; i++) {
                    assertEquals(i, offsets.get(i).longValue(), "offset gap at index " + i);
                }
            } finally {
                consumers.forEach(StreamConsumer::stop);
            }
        }
    }

    /**
     * stream create 幂等语义：同名同属性重复创建静默成功；同名不同属性由 broker 拒绝。
     */
    @Test
    void createIsIdempotentForSameProperties() {
        String topic = newTopic("create");
        StreamDefinition definition = new StreamDefinition(topic, PARTITIONS, Map.of());

        // 同名同属性：两次 create 都应成功
        provider.create(definition);
        provider.create(definition);

        // 同名不同属性：broker 拒绝（PRECONDITION_FAILED）
        StreamDefinition conflicting = new StreamDefinition(
                topic, PARTITIONS, Map.of("max-length-bytes", "100000"));
        StreamException exception = assertThrows(StreamException.class,
                () -> provider.create(conflicting));
        assertEquals(Constants.RESPONSE_CODE_PRECONDITION_FAILED, exception.getCode());
    }

    /**
     * Single Active Consumer：同名两个 consumer 订阅同一分区 stream，broker 保证同时只有一个
     * active；active 关闭后另一个异步接管。接管后不应重放阶段一已收（且已提交）的消息。
     */
    @Test
    void singleActiveConsumerExclusivityAndTakeover() throws Exception {
        String topic = newTopic("sac");
        String name = "it-sac";
        StreamDefinition definition = new StreamDefinition(topic, 1, Map.of());
        int phaseOne = 10;
        int phaseTwo = 10;

        provider.create(definition);
        try (StreamProducer producer = provider.producer(definition)) {
            // 同名构成 SAC 组（分组键为 (stream, name)）
            StreamConsumer first = provider.consumer(definition, 0, name);
            StreamConsumer second = provider.consumer(definition, 0, name);
            first.start();
            second.start();
            try {
                // 阶段一：SAC 激活是 broker 侧异步行为，Awaitility 等到其中一个收满
                for (int i = 0; i < phaseOne; i++) {
                    send(producer, topic, "p1-key-" + i, "p1-" + i);
                }
                List<ConsumerRecord> firstReceived = new ArrayList<>();
                List<ConsumerRecord> secondReceived = new ArrayList<>();
                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    firstReceived.addAll(first.poll(5, Duration.ofMillis(200)));
                    secondReceived.addAll(second.poll(5, Duration.ofMillis(200)));
                    return firstReceived.size() + secondReceived.size() >= phaseOne;
                });

                // 独占性：恰好一个收满，另一个一条都收不到
                boolean firstActive = firstReceived.size() == phaseOne && secondReceived.isEmpty();
                boolean secondActive = secondReceived.size() == phaseOne && firstReceived.isEmpty();
                assertTrue(firstActive || secondActive,
                        "exactly one consumer must be active, first=" + firstReceived.size()
                                + " second=" + secondReceived.size());

                StreamConsumer active = firstActive ? first : second;
                StreamConsumer standby = firstActive ? second : first;
                List<ConsumerRecord> standbyReceived = firstActive ? secondReceived : firstReceived;

                // 提交已收消息的 offset（与重启恢复同一套 tracker 语义）
                long maxOffset = (firstActive ? firstReceived : secondReceived).stream()
                        .mapToLong(ConsumerRecord::getOffset).max().orElseThrow();
                active.commit(maxOffset);
                active.stop();

                // 阶段二：接管是 broker 侧异步行为，新 active 从接管点起算 offset——
                // 接管窗口内发布的消息可能对新 active 不可见（实测： standby 收到 offset 10
                // 后直接跳到 12，窗口内的 11 被跳过）。循环重发编号消息直到全部收到。
                Set<String> expected = new HashSet<>();
                for (int i = 0; i < phaseTwo; i++) {
                    expected.add("p2-" + i);
                }
                await().atMost(Duration.ofSeconds(60)).until(() -> {
                    for (int i = 0; i < phaseTwo; i++) {
                        send(producer, topic, "p2-key-" + i, "p2-" + i);
                    }
                    standbyReceived.addAll(standby.poll(10, Duration.ofMillis(200)));
                    return standbyReceived.stream()
                            .map(RabbitStreamIT::payload).collect(Collectors.toSet())
                            .containsAll(expected);
                });

                // 接管后不重放阶段一已提交的消息
                Set<String> payloads = standbyReceived.stream()
                        .map(RabbitStreamIT::payload).collect(Collectors.toSet());
                for (int i = 0; i < phaseOne; i++) {
                    assertTrue(!payloads.contains("p1-" + i),
                            "phase-1 message p1-" + i + " must not be replayed after takeover");
                }
            } finally {
                first.stop();
                second.stop();
            }
        }
    }

    private static String payload(ConsumerRecord record) {
        return new String(record.getMessage().getPayload(), StandardCharsets.UTF_8);
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
     * 轮询所有分区直到收满 expected 条（Awaitility 超时 60s，远程共享 Docker 主机负载高时
     * broker 投递可能明显变慢）。
     */
    private static List<ConsumerRecord> pollUntil(List<StreamConsumer> consumers, int expected) {
        List<ConsumerRecord> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
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
