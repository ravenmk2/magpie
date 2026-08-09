package ravenworks.magpie.engine.impl.runtime;

import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.domain.entity.SourceEntity;
import ravenworks.magpie.domain.entity.TargetEntity;
import ravenworks.magpie.domain.entity.TopicEntity;
import ravenworks.magpie.domain.repository.ConsumerOffsetRepository;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.domain.repository.MessageLogRepository;
import ravenworks.magpie.domain.repository.RetryMessageRepository;
import ravenworks.magpie.domain.repository.SourceRepository;
import ravenworks.magpie.domain.repository.TargetRepository;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.source.http.TopicNotAllowedException;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.impl.election.LeaderElectionImpl;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamProvider;
import ravenworks.magpie.engine.impl.retry.RetryMessageStoreImpl;
import ravenworks.magpie.engine.impl.sink.SinkFactoryImpl;
import ravenworks.magpie.engine.impl.sink.TargetRegistryImpl;
import ravenworks.magpie.engine.impl.source.SourceFactoryImpl;
import ravenworks.magpie.engine.impl.source.SourceRegistryImpl;
import ravenworks.magpie.engine.impl.source.http.HttpSourceProvider;
import ravenworks.magpie.engine.impl.source.http.HttpSourceRouterImpl;
import ravenworks.magpie.engine.impl.stream.OffsetTrackerImpl;
import ravenworks.magpie.engine.impl.stream.RoutingStreamProducer;
import ravenworks.magpie.engine.impl.stream.StreamRegistryImpl;
import ravenworks.magpie.testsupport.RecordingSinkHandler;
import ravenworks.magpie.testsupport.RecordingSinkProvider;
import ravenworks.magpie.testsupport.TestJpa;
import ravenworks.magpie.testsupport.TestMySql;
import ravenworks.magpie.testsupport.TestRabbitMq;

import javax.sql.DataSource;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * 引擎级 e2e IT：真实 Coordinator + 真实 RabbitMQ Stream + 真实 MySQL，
 * 验证三种 DeliveryMode 的投递语义与重启恢复。
 *
 * <p>期望状态直接写库（magpie_topic / magpie_source / magpie_target），
 * 消息经 HttpSourceRouter 以 CloudEvent 灌入（subject=topic，xbusinesskey=businessKey），
 * 投递侧用 recording sink（{@link RecordingSinkProvider}）录制并注入故障。
 * topic 一律 partitions=1（全局有序），topic/source/target 名带随机后缀隔离用例。
 *
 * <p>重启用例（{@link #restartRedeliversUncommittedAndDrainsRetryStore}）会停掉共享
 * Coordinator，必须最后执行。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CoordinatorE2eIT {

    private static final Duration AWAIT = Duration.ofSeconds(30);
    private static final int RESYNC_INTERVAL_MS = 100;

    private static AnnotationConfigApplicationContext context;
    private static JdbcTemplate jdbc;
    private static LeaderLockRepository lockRepo;
    private static TopicRepository topicRepo;
    private static SourceRepository sourceRepo;
    private static TargetRepository targetRepo;
    private static RetryMessageRepository retryRepo;
    private static StreamRegistryImpl streamReg;
    private static SourceRegistryImpl sourceReg;
    private static TargetRegistryImpl targetReg;
    private static SourceFactoryImpl sourceFactory;
    private static SinkFactoryImpl sinkFactory;
    private static RabbitStreamProvider provider;
    private static RoutingStreamProducer producer;
    private static HttpSourceRouterImpl router;
    private static RetryMessageStore retryStore;
    private static RecordingSinkProvider recordingProvider;
    private static Coordinator coordinator;
    private static Coordinator restartedCoordinator;

    @BeforeAll
    static void setUp() {
        DataSource ds = TestMySql.reset();
        jdbc = new JdbcTemplate(ds);
        context = TestJpa.create(ds);
        lockRepo = context.getBean(LeaderLockRepository.class);
        topicRepo = context.getBean(TopicRepository.class);
        sourceRepo = context.getBean(SourceRepository.class);
        targetRepo = context.getBean(TargetRepository.class);
        retryRepo = context.getBean(RetryMessageRepository.class);
        var offsetRepo = context.getBean(ConsumerOffsetRepository.class);
        var msgLogRepo = context.getBean(MessageLogRepository.class);

        streamReg = new StreamRegistryImpl(topicRepo);
        sourceReg = new SourceRegistryImpl(sourceRepo);
        targetReg = new TargetRegistryImpl(targetRepo);
        var tracker = new OffsetTrackerImpl(offsetRepo);
        retryStore = new RetryMessageStoreImpl(msgLogRepo, retryRepo);
        provider = new RabbitStreamProvider(List.of(TestRabbitMq.streamUri()), tracker);
        producer = new RoutingStreamProducer(provider, streamReg);
        router = new HttpSourceRouterImpl();
        sourceFactory = new SourceFactoryImpl(List.of(new HttpSourceProvider(router)));
        recordingProvider = new RecordingSinkProvider(streamReg, retryStore);
        sinkFactory = new SinkFactoryImpl(List.of(recordingProvider));
        coordinator = startCoordinator();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (coordinator != null && coordinator.isRunning()) {
            coordinator.shutdown().get(60, TimeUnit.SECONDS);
        }
        if (restartedCoordinator != null && restartedCoordinator.isRunning()) {
            restartedCoordinator.shutdown().get(60, TimeUnit.SECONDS);
        }
        if (producer != null) {
            producer.close();
        }
        if (provider != null) {
            provider.close();
        }
        if (context != null) {
            context.close();
        }
    }

    @Test
    @Order(1)
    void orderedDeliversInOffsetOrderAndBlocksOnFailure() {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-ordered-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        seedTopology(topic, source, target, "ORDERED");
        var handler = awaitHandler(target);

        // 第一批 20 条全部成功：按 offset 序收齐，payload 序即灌入序
        for (int i = 0; i < 20; i++) {
            publish(source, topic, String.valueOf(i), null);
        }
        await().atMost(AWAIT).until(() -> handler.received().size() >= 20);
        var first = handler.received();
        assertEquals(20, first.size(), "ORDERED 成功路径不应有重复尝试");
        for (int i = 0; i < 20; i++) {
            assertEquals(i, first.get(i).getOffset(), "offset 应按序推进");
            assertEquals(String.valueOf(i), payload(first.get(i)));
        }

        // 第二批：序号 20 持续失败——阻塞原地重试，不跳过、不落 RetryStore
        handler.failWhen(payloadIs("20"));
        for (int i = 20; i < 26; i++) {
            publish(source, topic, String.valueOf(i), null);
        }
        await().atMost(AWAIT).until(() -> handler.receivedPayloads().contains("20"));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> !handler.receivedPayloads().contains("21"));
        assertEquals(0, retryCount(target + "-0"), "ORDERED 失败消息不落 RetryStore");

        // 恢复后按序补齐 20..25
        handler.clearFailures();
        await().atMost(AWAIT).until(() -> {
            var payloads = handler.receivedPayloads();
            for (int i = 20; i < 26; i++) {
                if (!payloads.contains(String.valueOf(i))) {
                    return false;
                }
            }
            return true;
        });
        var tail = handler.receivedPayloads();
        int from = tail.indexOf("20");
        var deduped = new LinkedHashSet<>(tail.subList(from, tail.size()));
        assertEquals(List.of("20", "21", "22", "23", "24", "25"), List.copyOf(deduped));
    }

    @Test
    @Order(2)
    void keyOrderedIsolatesFailedKeyAndRedeliversInOrder() {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-keyordered-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        String worker = target + "-0";
        seedTopology(topic, source, target, "KEY_ORDERED");
        var handler = awaitHandler(target);

        // A1 持续失败：A1 落库并阻塞 keyA，A2 被分流落库（不经 handler），keyB 照常投递
        handler.failWhen(payloadIs("A1"));
        for (int i = 0; i < 3; i++) {
            publish(source, topic, "A" + i, "keyA");
            publish(source, topic, "B" + i, "keyB");
        }
        await().atMost(AWAIT).until(() -> handler.receivedPayloads()
                .containsAll(List.of("B0", "B1", "B2")));
        await().atMost(AWAIT).until(() -> retryCount(worker, "keyA") == 2);
        assertFalse(handler.receivedPayloads().contains("A2"),
                "keyA 阻塞后 A2 应直接分流落库，不经 handler");

        // 恢复后 keyA 从 RetryStore 按 offset 序排空
        handler.clearFailures();
        await().atMost(AWAIT).until(() -> retryCount(worker) == 0);
        assertEquals(List.of("A0", "A1", "A2"),
                firstOccurrenceOrder(handler.receivedPayloads(), "A"),
                "同 key 内顺序不能乱（含 RetryStore 重投）");
    }

    @Test
    @Order(3)
    void bestEffortRetriesWithoutBlockingOthers() {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-besteffort-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        String worker = target + "-0";
        seedTopology(topic, source, target, "BEST_EFFORT");
        var handler = awaitHandler(target);

        // 只有序号 3 失败：其余 9 条照常送达（不阻塞），失败条落库
        handler.failWhen(payloadIs("3"));
        for (int i = 0; i < 10; i++) {
            publish(source, topic, String.valueOf(i), null);
        }
        await().atMost(AWAIT).until(() -> {
            var delivered = new LinkedHashSet<>(handler.receivedPayloads());
            return delivered.containsAll(List.of("0", "1", "2", "4", "5", "6", "7", "8", "9"));
        });
        await().atMost(AWAIT).until(() -> retryCount(worker) == 1);

        // 恢复后失败条在空闲窗口重试成功，最终 10 条全部送达
        handler.clearFailures();
        await().atMost(AWAIT).until(() -> retryCount(worker) == 0
                && handler.receivedPayloads().contains("3"));
        assertEquals(10, new LinkedHashSet<>(handler.receivedPayloads()).size());
    }

    @Test
    @Order(4)
    void restartDrainsRetryStoreAfterRelaunch() throws Exception {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-restart-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        String worker = target + "-0";
        seedTopology(topic, source, target, "KEY_ORDERED");
        var handler = awaitHandler(target);

        // keyA 全部失败：首条落库后 keyA 阻塞，后续分流，6 条全部积压到 RetryStore
        handler.failWhen(payloadStartsWith("A"));
        for (int i = 0; i < 6; i++) {
            publish(source, topic, "A" + i, "keyA");
        }
        await().atMost(AWAIT).until(() -> retryCount(worker, "keyA") == 6);

        // 停机（只停连接器，provider 不关，offset 已随关停提交到 DB）
        coordinator.shutdown().get(60, TimeUnit.SECONDS);

        // 同 DB 同 provider 重启一套引擎：重建的 handler 不带失败规则（下游已恢复）
        restartedCoordinator = startCoordinator();
        await().atMost(AWAIT).until(() -> recordingProvider.handlers(target).size() >= 2);
        RecordingSinkHandler restartedHandler = recordingProvider.latestHandler(target);
        assertNotNull(restartedHandler);

        // 存量按 offset 序排空到新的 handler，无丢失
        await().atMost(AWAIT).until(() -> retryCount(worker) == 0
                && restartedHandler.received().size() >= 6);
        assertEquals(List.of("A0", "A1", "A2", "A3", "A4", "A5"),
                firstOccurrenceOrder(restartedHandler.receivedPayloads(), "A"));

        restartedCoordinator.shutdown().get(60, TimeUnit.SECONDS);
    }


    private static Coordinator startCoordinator() {
        var c = new Coordinator(new LeaderElectionImpl(lockRepo), streamReg, provider,
                sourceReg, sourceFactory, targetReg, sinkFactory, producer, RESYNC_INTERVAL_MS);
        c.start();
        return c;
    }

    /**
     * 播种期望状态并立即触发收敛，然后等 source 在 router 上订阅生效。
     * 探针用不在 allowedTopics 里的 subject：订阅后抛 TopicNotAllowedException
     * （未订阅是 NoSubscriberException），不向 stream 注入垃圾消息。
     */
    private static void seedTopology(String topic, String source, String target, String deliveryMode) {
        var topicEntity = new TopicEntity();
        topicEntity.setId(Uuids.uuid7Hex());
        topicEntity.setName(topic);
        topicEntity.setTitle(topic);
        topicEntity.setPartitions(1);
        topicEntity.setProperties(Map.of());
        topicRepo.save(topicEntity);

        var sourceEntity = new SourceEntity();
        sourceEntity.setId(Uuids.uuid7Hex());
        sourceEntity.setType("http");
        sourceEntity.setName(source);
        sourceEntity.setTitle(source);
        sourceEntity.setEnabled(true);
        sourceEntity.setProperties(Map.of("allowedTopics", List.of(topic)));
        sourceRepo.save(sourceEntity);

        var targetEntity = new TargetEntity();
        targetEntity.setId(Uuids.uuid7Hex());
        targetEntity.setType("recording");
        targetEntity.setName(target);
        targetEntity.setTitle(target);
        targetEntity.setTopic(topic);
        targetEntity.setEnabled(true);
        targetEntity.setProperties(Map.of(
                "deliveryMode", deliveryMode,
                "commit.interval", 500));
        targetRepo.save(targetEntity);

        coordinator.wake();

        var probe = CloudEventBuilder.v1()
                .withId(Uuids.uuid7Hex())
                .withSource(URI.create("e2e"))
                .withType("e2e.probe")
                .withSubject(topic + ".probe")
                .build();
        await().atMost(AWAIT).until(() -> {
            try {
                router.publish(source, probe).join();
                return false;
            } catch (CompletionException e) {
                return e.getCause() instanceof TopicNotAllowedException;
            }
        });
    }

    private static RecordingSinkHandler awaitHandler(String target) {
        await().atMost(AWAIT).until(() -> recordingProvider.latestHandler(target) != null);
        return recordingProvider.latestHandler(target);
    }

    private static void publish(String source, String topic, String payload, String businessKey) {
        var builder = CloudEventBuilder.v1()
                .withId(Uuids.uuid7Hex())
                .withSource(URI.create("e2e"))
                .withType("e2e.test")
                .withSubject(topic)
                .withData("text/plain", payload.getBytes(StandardCharsets.UTF_8));
        if (businessKey != null) {
            builder.withExtension("xbusinesskey", businessKey);
        }
        router.publish(source, builder.build()).join();
    }

    private static int retryCount(String consumer) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM magpie_retry_message WHERE consumer = ?",
                Integer.class, consumer);
        return count != null ? count : 0;
    }

    private static int retryCount(String consumer, String businessKey) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM magpie_retry_message WHERE consumer = ? AND business_key = ?",
                Integer.class, consumer, businessKey);
        return count != null ? count : 0;
    }

    private static String payload(ConsumerRecord record) {
        return new String(record.getMessage().getPayload(), StandardCharsets.UTF_8);
    }

    private static Predicate<ConsumerRecord> payloadIs(String expected) {
        return r -> expected.equals(payload(r));
    }

    private static Predicate<ConsumerRecord> payloadStartsWith(String prefix) {
        return r -> payload(r).startsWith(prefix);
    }

    /**
     * 按前缀过滤后取首次出现顺序（ORDERED 原地重试、RetryStore 重投会造成重复尝试）。
     */
    private static List<String> firstOccurrenceOrder(List<String> payloads, String prefix) {
        var seen = new LinkedHashSet<String>();
        payloads.stream().filter(p -> p.startsWith(prefix)).forEach(seen::add);
        return List.copyOf(seen);
    }

}
