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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
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
        provider = new RabbitStreamProvider(TestRabbitMq.streamOptions(), OFFSET_TRACKER);
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

    /**
     * 跨实例续传：发送成功、deleteBatch 未执行就宕机 → 后继实例重投（at-least-once 重复）。
     * 确定性实现：BEFORE DELETE 触发器让 connector1 删行必失败（SIGNAL SQLSTATE '45000'），
     * 等价于"发送后宕机"的现场，不靠时序赌博；拆触发器后 connector2 接管重投并删行。
     */
    @Test
    void redeliversWhenKilledBetweenSendAndDelete() throws Exception {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String stream = "it-crash-" + suffix;
        var definition = new StreamDefinition(stream, 1, Map.of());
        provider.create(definition);

        // created_at 显式写成过去时间绕过 readLag
        insertOutboxRows(stream, LocalDateTime.now().minusSeconds(120), 3);

        var consumer = newConsumer(definition, "it-crash-consumer-" + suffix);
        consumer.start();
        try (StreamProducer producer = provider.producer(definition)) {
            // 触发器先就位：connector1 发送成功后 deleteBatch 必失败，行留在表内
            // （test 用户无 SUPER，binlog 开启时 CREATE TRIGGER 报 1419，DDL 走 root 连接）
            executeAsRoot("CREATE TRIGGER it_sabotage_delete BEFORE DELETE ON " + TABLE
                    + " FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sabotaged delete'");
            var connector1 = newConnector(producer, "it-mysql-crash1-" + suffix, 200, 500);
            List<ConsumerRecord> received = new ArrayList<>();
            try {
                connector1.start();
                try {
                    pollUntil(consumer, received, 3);
                    // 已发送（consumer 收到）但删除被触发器拦截：行还在表里
                    assertEquals(3, outboxCount(stream));
                } finally {
                    // 宕机：直接弃置 connector1
                    connector1.shutdown().get(30, TimeUnit.SECONDS);
                }
            } finally {
                executeAsRoot("DROP TRIGGER IF EXISTS it_sabotage_delete");
            }
            // 宕机现场快照：每个 message id 各收到 connector1 的一份
            var perIdAfterCrash = countById(received);
            assertEquals(3, perIdAfterCrash.size());

            // connector2 接管同一 outbox 表、同一 stream：重投同 id 消息后删行成功
            var connector2 = newConnector(producer, "it-mysql-crash2-" + suffix, 200, 500);
            connector2.start();
            try {
                await().atMost(AWAIT).until(() -> {
                    received.addAll(consumer.poll(10, Duration.ofMillis(200)));
                    var perId = countById(received);
                    return outboxCount(stream) == 0
                            && perIdAfterCrash.keySet().stream().allMatch(
                            id -> perId.getOrDefault(id, 0L) > perIdAfterCrash.get(id));
                });
                // ① consumer 从 offset 0 读到全部 payload 至少一次
                assertEquals(0, received.getFirst().getOffset());
                var payloads = received.stream()
                        .map(r -> new String(r.getMessage().getPayload(), StandardCharsets.UTF_8))
                        .toList();
                assertTrue(payloads.containsAll(List.of("payload-0", "payload-1", "payload-2")));
                // ② 重复消息 message id 相同：connector1 发送的与 connector2 重投的同 id
                //    （上面的 until 已保证每个 id 在 connector2 接管后至少再收到一份，
                //      且 outbox 清空证明 connector2 完成"重投 + 删除"全链路）
            } finally {
                connector2.shutdown().get(30, TimeUnit.SECONDS);
            }
        } finally {
            consumer.stop();
        }
    }

    /**
     * 未提交事务不可见 + created_at 走 DB 默认值（DB 时钟路径，未被其他用例覆盖）。
     * InnoDB 默认隔离级下，未提交行对 poller 连接不可见——这是 readLag 设计的根基。
     */
    @Test
    void uncommittedRowsAreInvisibleUntilCommit() throws Exception {
        String suffix = Uuids.uuidHex().substring(0, 8);
        String stream = "it-tx-" + suffix;
        var definition = new StreamDefinition(stream, 1, Map.of());
        provider.create(definition);

        // 独立连接手工事务：不写 created_at，走 DEFAULT CURRENT_TIMESTAMP(3)，先不提交
        var txConnection = dataSource.getConnection();
        txConnection.setAutoCommit(false);
        try {
            try (var ps = txConnection.prepareStatement(
                    "INSERT INTO " + TABLE
                            + " (id, type, event_time, topic, tenant_id, business_key, headers, payload)"
                            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    ps.setString(1, Uuids.uuid7Hex());
                    ps.setString(2, "it.outbox");
                    ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
                    ps.setString(4, stream);
                    ps.setString(5, "it-tenant");
                    ps.setString(6, "bk-" + i);
                    ps.setString(7, "{}");
                    ps.setString(8, "payload-" + i);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            var consumer = newConsumer(definition, "it-tx-consumer-" + suffix);
            consumer.start();
            try (StreamProducer producer = provider.producer(definition)) {
                var connector = newConnector(producer, "it-mysql-tx-" + suffix, 200, 500);
                connector.start();
                try {
                    List<ConsumerRecord> received = new ArrayList<>();
                    // 超过 readLag + 若干 poll 周期：未提交行对 poller 与其他连接均不可见
                    await().during(Duration.ofSeconds(2)).atMost(AWAIT).until(() -> {
                        received.addAll(consumer.poll(10, Duration.ofMillis(100)));
                        return received.isEmpty() && outboxCount(stream) == 0;
                    });

                    txConnection.commit();

                    // 提交后立即可见（created_at 早已越过 readLag），按插入序送达并删除
                    pollUntil(consumer, received, 3);
                    for (int i = 0; i < 3; i++) {
                        assertEquals("payload-" + i,
                                new String(received.get(i).getMessage().getPayload(),
                                        StandardCharsets.UTF_8));
                    }
                    await().atMost(AWAIT).until(() -> outboxCount(stream) == 0);
                } finally {
                    connector.shutdown().get(30, TimeUnit.SECONDS);
                }
            } finally {
                consumer.stop();
            }
        } finally {
            txConnection.close();
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
        pollUntil(consumer, received, expected);
        return received;
    }

    private static void pollUntil(StreamConsumer consumer, List<ConsumerRecord> received, int expected) {
        await().atMost(AWAIT).until(() -> {
            received.addAll(consumer.poll(10, Duration.ofMillis(200)));
            return received.size() >= expected;
        });
    }

    private static Map<String, Long> countById(List<ConsumerRecord> records) {
        var counts = new HashMap<String, Long>();
        for (var record : records) {
            counts.merge(record.getMessage().getId(), 1L, Long::sum);
        }
        return counts;
    }

    /**
     * test 用户无 SUPER 权限，binlog 开启时 CREATE TRIGGER 被 MySQL 1419 拒绝；
     * Testcontainers 容器的 root 密码与 test 用户相同（MYSQL_ROOT_PASSWORD=password），
     * 触发器 DDL 走 root 连接执行。
     */
    private static void executeAsRoot(String sql) throws SQLException {
        try (var connection = DriverManager.getConnection(
                dataSource.getUrl(), "root", dataSource.getPassword());
             var st = connection.createStatement()) {
            st.execute(sql);
        }
    }

}
