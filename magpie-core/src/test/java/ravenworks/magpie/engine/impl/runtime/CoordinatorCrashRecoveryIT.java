package ravenworks.magpie.engine.impl.runtime;

import io.cloudevents.core.builder.CloudEventBuilder;
import org.junit.jupiter.api.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.domain.entity.SourceEntity;
import ravenworks.magpie.domain.entity.TargetEntity;
import ravenworks.magpie.domain.entity.TopicEntity;
import ravenworks.magpie.domain.repository.*;
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
import ravenworks.magpie.testsupport.*;

import javax.sql.DataSource;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * 引擎级崩溃恢复与动态治理 e2e IT（装配模式同 {@link CoordinatorE2eIT}）：
 *
 * <p>场景一（{@link #crashRecoveryRedeliversAndDrains}）模拟进程崩溃而非优雅停机：
 * 把 commit.interval 调到 600s 保证消费期间不发生任何 offset 提交，然后直接
 * {@code provider.close()} 关掉 RabbitMQ Environment——连接与订阅瞬间死亡，
 * 不经过 Coordinator/SinkWorker 的优雅停机路径（PRE_SHUTDOWN 的 offset 提交不会发生）。
 * 旧 Coordinator 对象仍留在 JVM 里空转（poll 永远取空、无任何新的投递与提交），
 * 与真实崩溃的差异仅是它还保持着 leader 锁心跳；但同 JVM 下 InstanceId 相同，
 * 新引擎的选举可按"同实例重取"直接接管锁（见 LeaderLockRepository.acquireLock），
 * 不影响接管语义。随后用新 provider + 新 Coordinator 接管同一 DB 同一 broker，
 * 验证未提交消息全量重投、RetryStore 存量排空、同 key 顺序不乱（at-least-once 允许重复）。
 *
 * <p>场景二（{@link #disableEnableResumesFromCommittedOffset}）直接 SQL 改
 * magpie_target.is_enabled，验证禁用期积压、启用后从已提交 offset 续传（不全量重放）、
 * 旧订阅确已拆除。
 *
 * <p>场景一会把共享 provider 关掉并留下僵尸 Coordinator，必须最后执行。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CoordinatorCrashRecoveryIT {

    private static final Duration AWAIT = Duration.ofSeconds(60);
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
    private static RabbitStreamProvider recoveryProvider;
    private static RoutingStreamProducer recoveryProducer;
    private static Coordinator recoveryCoordinator;

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
        provider = new RabbitStreamProvider(TestRabbitMq.streamOptions(), tracker);
        producer = new RoutingStreamProducer(provider, streamReg);
        router = new HttpSourceRouterImpl();
        sourceFactory = new SourceFactoryImpl(List.of(new HttpSourceProvider(router)));
        recordingProvider = new RecordingSinkProvider(streamReg, retryStore);
        sinkFactory = new SinkFactoryImpl(List.of(recordingProvider));
        coordinator = startCoordinator(provider, producer);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (coordinator != null && coordinator.isRunning()) {
            coordinator.shutdown().get(60, TimeUnit.SECONDS);
        }
        if (recoveryCoordinator != null && recoveryCoordinator.isRunning()) {
            recoveryCoordinator.shutdown().get(60, TimeUnit.SECONDS);
        }
        if (producer != null) {
            producer.close();
        }
        if (recoveryProducer != null) {
            recoveryProducer.close();
        }
        closeQuietly(provider);
        closeQuietly(recoveryProvider);
        if (context != null) {
            context.close();
        }
    }

    @Test
    @Order(1)
    void disableEnableResumesFromCommittedOffset() {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-disable-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        seedTopology(topic, source, target, "ORDERED", 500);
        var handler = awaitHandler(target);

        // 10 条全部送达，并等 commit.interval 节拍把 offset=10 提交到 DB
        for (int i = 0; i < 10; i++) {
            publish(source, topic, String.valueOf(i), null);
        }
        await().atMost(AWAIT).until(() -> handler.received().size() >= 10);
        await().atMost(AWAIT).until(() -> committedOffset(target) == 10);

        // 直接 SQL 禁用 target：reconcile 节拍退役 sink（优雅关停，offset 已是 10）。
        // 用 during 确认计数稳定——既是"禁用期间无新投递"的断言，也给退役留出收敛窗口
        jdbc.update("UPDATE magpie_target SET is_enabled = 0 WHERE name = ?", target);
        coordinator.wake();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> handler.received().size() == 10);

        // 禁用期再发 5 条：积压在 stream，不得投递
        for (int i = 10; i < 15; i++) {
            publish(source, topic, String.valueOf(i), null);
        }
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> handler.received().size() == 10);

        // 重新启用：新 connector 从 committed offset=10 续传，恰好收到积压的 5 条
        jdbc.update("UPDATE magpie_target SET is_enabled = 1 WHERE name = ?", target);
        coordinator.wake();
        await().atMost(AWAIT).until(() -> recordingProvider.handlers(target).size() >= 2);
        RecordingSinkHandler newHandler = recordingProvider.latestHandler(target);
        assertNotNull(newHandler);
        await().atMost(AWAIT).until(() -> newHandler.received().size() >= 5);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
                .until(() -> newHandler.received().size() == 5);
        assertEquals(List.of("10", "11", "12", "13", "14"), newHandler.receivedPayloads(),
                "续传应恰好是积压的 5 条，从 committed offset 继续而非全量重放");
        assertEquals(10, handler.received().size(), "旧订阅应已拆除，不再收到任何消息");
    }

    @Test
    @Order(2)
    void crashRecoveryRedeliversAndDrains() {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String topic = "e2e-crash-" + suffix;
        String source = "e2e-src-" + suffix;
        String target = "e2e-target-" + suffix;
        String worker = target + "-0";
        // commit.interval=600s：消费期间绝不提交 offset，模拟崩溃前没来得及提交
        seedTopology(topic, source, target, "KEY_ORDERED", 600_000);
        var handler = awaitHandler(target);

        // keyA 全部失败落 RetryStore；keyB 正常送达（但也未提交 offset）
        handler.failWhen(payloadStartsWith("A"));
        List<String> messageIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messageIds.add(publish(source, topic, "A" + i, "keyA"));
            messageIds.add(publish(source, topic, "B" + i, "keyB"));
        }
        await().atMost(AWAIT).until(() -> retryCount(worker, "keyA") == 5
                && handler.receivedPayloads().containsAll(List.of("B0", "B1", "B2", "B3", "B4")));
        assertEquals(-1, committedOffset(target), "崩溃前 offset 不应有任何提交");

        // 崩溃：直接关掉 RabbitMQ Environment，不走任何优雅停机（无 offset 提交）。
        // 旧 Coordinator 不 shutdown，留在 JVM 里空转（详见类注释）
        provider.close();

        // 新 provider + 新 Coordinator 接管同一个 DB 和同一个 broker
        var tracker = new OffsetTrackerImpl(context.getBean(ConsumerOffsetRepository.class));
        recoveryProvider = new RabbitStreamProvider(TestRabbitMq.streamOptions(), tracker);
        recoveryProducer = new RoutingStreamProducer(recoveryProvider, streamReg);
        recoveryCoordinator = startCoordinator(recoveryProvider, recoveryProducer);

        // 接管后新建 handler（不带失败规则，下游视为已恢复）
        await().atMost(AWAIT).until(() -> recordingProvider.handlers(target).size() >= 2);
        RecordingSinkHandler recovered = recordingProvider.latestHandler(target);
        assertNotNull(recovered);

        // ① 全部消息最终送达（按 messageId 去重后齐全）；② RetryStore 最终排空
        await().atMost(AWAIT).until(() -> retryCount(worker) == 0
                && distinctMessageIds(recovered).containsAll(messageIds));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10))
                .until(() -> retryCount(worker) == 0);

        // ③ 同 key 按首次出现顺序不乱（含 stream 重投与 RetryStore 重投的重复尝试）
        assertEquals(List.of("A0", "A1", "A2", "A3", "A4"),
                firstOccurrenceOrder(recovered.receivedPayloads(), "A"));
        assertEquals(List.of("B0", "B1", "B2", "B3", "B4"),
                firstOccurrenceOrder(recovered.receivedPayloads(), "B"));
    }


    private static Coordinator startCoordinator(RabbitStreamProvider streamProvider,
                                                RoutingStreamProducer streamProducer) {
        var c = new Coordinator(new LeaderElectionImpl(lockRepo), streamReg, streamProvider,
                sourceReg, sourceFactory, targetReg, sinkFactory, streamProducer, RESYNC_INTERVAL_MS);
        c.start();
        return c;
    }

    private static void closeQuietly(RabbitStreamProvider p) {
        if (p == null) {
            return;
        }
        try {
            p.close();
        } catch (Exception e) {
            // 场景一已关过共享 provider，重复关闭失败不影响测试结果
        }
    }

    /**
     * 播种期望状态并立即触发收敛，然后等 source 在 router 上订阅生效（探针同
     * {@link CoordinatorE2eIT}）。commitIntervalMs 写入 target 的 commit.interval。
     */
    private static void seedTopology(String topic, String source, String target,
                                     String deliveryMode, long commitIntervalMs) {
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
                "commit.interval", commitIntervalMs));
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

    /**
     * 灌入一条消息并返回其 messageId（CloudEvent id 全链路透传为 message_id）。
     */
    private static String publish(String source, String topic, String payload, String businessKey) {
        String id = Uuids.uuid7Hex();
        var builder = CloudEventBuilder.v1()
                .withId(id)
                .withSource(URI.create("e2e"))
                .withType("e2e.test")
                .withSubject(topic)
                .withData("text/plain", payload.getBytes(StandardCharsets.UTF_8));
        if (businessKey != null) {
            builder.withExtension("xbusinesskey", businessKey);
        }
        router.publish(source, builder.build()).join();
        return id;
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

    /**
     * DB 中已提交的 offset（OffsetTracker 写入的是下一条待消费位置）。
     * consumer 启动时 read 即建行（-1），无行会抛 EmptyResultDataAccessException。
     */
    private static long committedOffset(String consumer) {
        Long offset = jdbc.queryForObject(
                "SELECT `offset` FROM magpie_consumer_offset WHERE name = ? AND `partition` = 0",
                Long.class, consumer);
        return offset != null ? offset : -1;
    }

    private static Set<String> distinctMessageIds(RecordingSinkHandler handler) {
        return handler.received().stream()
                .map(r -> r.getMessage().getId())
                .collect(Collectors.toSet());
    }

    private static String payload(ConsumerRecord record) {
        return new String(record.getMessage().getPayload(), StandardCharsets.UTF_8);
    }

    private static Predicate<ConsumerRecord> payloadStartsWith(String prefix) {
        return r -> payload(r).startsWith(prefix);
    }

    /**
     * 按前缀过滤后取首次出现顺序（重投会造成重复尝试）。
     */
    private static List<String> firstOccurrenceOrder(List<String> payloads, String prefix) {
        var seen = new LinkedHashSet<String>();
        payloads.stream().filter(p -> p.startsWith(prefix)).forEach(seen::add);
        return List.copyOf(seen);
    }

}
