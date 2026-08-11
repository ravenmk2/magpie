package ravenworks.magpie.engine.impl.source.mysql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.api.stream.*;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamProvider;
import ravenworks.magpie.testsupport.TestMySql;
import ravenworks.magpie.testsupport.TestRabbitMq;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * MySQL Source（outbox 轮询）IT：真实 MySQL outbox 表 + 真实 RabbitMQ Stream。
 * 表结构来自 docs/database/source-mysql.sql（幂等），不用内联 DDL——
 * IT 的目的之一是验证这份脚本能在真实 MySQL 上跑通。
 *
 * <p>Connector 直起（不经 Coordinator）；每个用例用独立 stream 隔离，
 * 表级断言按 topic 过滤。OffsetTracker 用内存 fake（本 IT 不覆盖 offset 提交边界）。
 */
class MySqlPollSourceIT {

    private static final Duration AWAIT = Duration.ofSeconds(30);
    private static final Path OUTBOX_SQL = Path.of("..", "docs", "database", "source-mysql.sql");
    private static final String TABLE = "magpie_outbox_message";

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

    private static DriverManagerDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static RabbitStreamProvider provider;

    @BeforeAll
    static void setUp() throws Exception {
        dataSource = (DriverManagerDataSource) TestMySql.reset();
        try (var connection = dataSource.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource(OUTBOX_SQL));
        }
        jdbc = new JdbcTemplate(dataSource);
        provider = new RabbitStreamProvider(List.of(TestRabbitMq.streamUri()), OFFSET_TRACKER);
    }

    @AfterAll
    static void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    void pollsOutboxRowsIntoStreamAndDeletesThem() throws Exception {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String stream = "it-outbox-" + suffix;
        var definition = new StreamDefinition(stream, 1, Map.of());
        provider.create(definition);

        // created_at 显式写成过去时间绕过 readLag；按秒递增保证 (created_at, id) 序
        var base = LocalDateTime.now().minusSeconds(120);
        insertOutboxRows(stream, base, 10);

        var consumer = newConsumer(definition, "it-outbox-consumer-" + suffix);
        consumer.start();
        try (StreamProducer producer = provider.producer(definition)) {
            var connector = newConnector(producer, "it-mysql-poll-" + suffix, 200, 500);
            connector.start();
            try {
                List<ConsumerRecord> received = pollUntil(consumer, 10);
                for (int i = 0; i < 10; i++) {
                    var message = received.get(i).getMessage();
                    assertEquals("payload-" + i,
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                    assertEquals("bk-" + i, message.getBusinessKey());
                    assertEquals(stream, message.getTopic());
                }
                await().atMost(AWAIT).until(() -> outboxCount(stream) == 0);
            } finally {
                connector.shutdown().get(30, TimeUnit.SECONDS);
            }
        } finally {
            consumer.stop();
        }
    }

    @Test
    void respectsReadLagForFreshRows() throws Exception {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String stream = "it-readlag-" + suffix;
        var definition = new StreamDefinition(stream, 1, Map.of());
        provider.create(definition);

        // created_at=now 的"新"行：readLag=1500ms 窗口内不应被读到
        insertOutboxRows(stream, LocalDateTime.now(), 1);

        var consumer = newConsumer(definition, "it-readlag-consumer-" + suffix);
        consumer.start();
        try (StreamProducer producer = provider.producer(definition)) {
            var connector = newConnector(producer, "it-mysql-readlag-" + suffix, 200, 1500);
            connector.start();
            try {
                List<ConsumerRecord> early = new ArrayList<>();
                long deadline = System.nanoTime() + Duration.ofMillis(600).toNanos();
                while (System.nanoTime() < deadline) {
                    early.addAll(consumer.poll(10, Duration.ofMillis(100)));
                }
                assertTrue(early.isEmpty(), "readLag 窗口内新行不应被投递");

                List<ConsumerRecord> received = pollUntil(consumer, 1);
                assertEquals("payload-0",
                        new String(received.get(0).getMessage().getPayload(), StandardCharsets.UTF_8));
                await().atMost(AWAIT).until(() -> outboxCount(stream) == 0);
            } finally {
                connector.shutdown().get(30, TimeUnit.SECONDS);
            }
        } finally {
            consumer.stop();
        }
    }


    private static StreamConsumer newConsumer(StreamDefinition definition, String name) {
        return provider.consumer(definition, name).getFirst();
    }

    private static MySqlPollSourceConnector newConnector(StreamProducer producer,
                                                         String name,
                                                         int pollInterval,
                                                         int readLag) {
        return new MySqlPollSourceConnector(
                producer, name, Map.of(
                "url", dataSource.getUrl(),
                "username", dataSource.getUsername(),
                "password", dataSource.getPassword(),
                "tableName", TABLE,
                "batchSize", 100,
                "pollInterval", pollInterval,
                "readLag", readLag,
                "sendTimeout", 10000,
                "sendStrategy", "ordered"));
    }

    private static void insertOutboxRows(String topic, LocalDateTime firstCreatedAt, int count)
            throws Exception {
        try (var connection = dataSource.getConnection();
             var ps = connection.prepareStatement(
                     "INSERT INTO " + TABLE
                             + " (id, type, event_time, topic, tenant_id, business_key, headers, payload, created_at)"
                             + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            for (int i = 0; i < count; i++) {
                ps.setString(1, Uuids.uuid7Hex());
                ps.setString(2, "it.outbox");
                ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                ps.setString(4, topic);
                ps.setString(5, "it-tenant");
                ps.setString(6, "bk-" + i);
                ps.setString(7, "{}");
                ps.setString(8, "payload-" + i);
                ps.setTimestamp(9, Timestamp.valueOf(firstCreatedAt.plusSeconds(i)));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static int outboxCount(String topic) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE topic = ?",
                Integer.class, topic);
        return count != null ? count : 0;
    }

    private static List<ConsumerRecord> pollUntil(StreamConsumer consumer, int expected) {
        List<ConsumerRecord> received = new ArrayList<>();
        await().atMost(AWAIT).until(() -> {
            received.addAll(consumer.poll(10, Duration.ofMillis(200)));
            return received.size() >= expected;
        });
        return received;
    }

}
