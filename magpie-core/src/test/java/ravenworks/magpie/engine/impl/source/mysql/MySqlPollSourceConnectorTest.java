package ravenworks.magpie.engine.impl.source.mysql;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class MySqlPollSourceConnectorTest {

    private static final String URL = "jdbc:h2:mem:mysql_src;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);


    static class FakeStreamProducer implements StreamProducer {

        final List<MessageRecord> sent = new CopyOnWriteArrayList<>();
        private final Queue<CompletableFuture<SendResult>> script = new ConcurrentLinkedQueue<>();

        void thenReturn(CompletableFuture<SendResult> result) {
            this.script.add(result);
        }

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            this.sent.add(record);
            var result = this.script.poll();
            return result != null
                    ? result
                    : CompletableFuture.completedFuture(new SendResult().setSucceeded(true).setMessage(record));
        }

        @Override
        public void close() {
        }

    }


    private FakeStreamProducer producer;
    private MySqlPollSourceConnector connector;

    @BeforeEach
    void setUp() throws SQLException {
        try (var c = DriverManager.getConnection(URL, "sa", ""); var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS magpie_outbox_message");
            st.execute("""
                    CREATE TABLE magpie_outbox_message (
                        id CHAR(32) NOT NULL PRIMARY KEY,
                        type VARCHAR(128) NOT NULL DEFAULT '',
                        event_time DATETIME(3) NOT NULL,
                        topic VARCHAR(128) NOT NULL DEFAULT '',
                        tenant_id VARCHAR(64) NOT NULL DEFAULT '',
                        business_key VARCHAR(128) NOT NULL DEFAULT '',
                        headers VARCHAR(2048) NOT NULL,
                        payload MEDIUMTEXT NOT NULL,
                        created_at DATETIME(3) NOT NULL
                    )
                    """);
        }
        this.producer = new FakeStreamProducer();
    }

    @AfterEach
    void tearDown() {
        if (this.connector != null) {
            this.connector.shutdown().join();
        }
    }

    private void start(String sendStrategy) {
        this.start(sendStrategy, Map.of());
    }

    private void start(String sendStrategy, Map<String, Object> overrides) {
        var properties = new HashMap<String, Object>();
        properties.put("url", URL);
        properties.put("username", "sa");
        properties.put("password", "");
        properties.put("pollInterval", 50);
        properties.put("retryDelay", 200);
        properties.put("readLag", 500);
        properties.put("sendTimeout", 5_000);
        properties.put("sendStrategy", sendStrategy);
        properties.putAll(overrides);
        this.connector = new MySqlPollSourceConnector(this.producer, "src", properties);
        this.connector.start();
    }

    private static void failNext(FakeStreamProducer producer) {
        producer.thenReturn(CompletableFuture.completedFuture(
                new SendResult().setSucceeded(false).setError("broker rejected")));
    }

    private static void insertRow(String id, String businessKey, LocalDateTime createdAt) throws SQLException {
        try (var c = DriverManager.getConnection(URL, "sa", "");
             var ps = c.prepareStatement("""
                     INSERT INTO magpie_outbox_message
                         (id, type, event_time, topic, tenant_id, business_key, headers, payload, created_at)
                     VALUES (?, 't.order', ?, 'orders', 't1', ?, '{"h":"v"}', '{"x":1}', ?)
                     """)) {
            ps.setString(1, id);
            ps.setTimestamp(2, Timestamp.valueOf(createdAt));
            ps.setString(3, businessKey);
            ps.setTimestamp(4, Timestamp.valueOf(createdAt));
            ps.executeUpdate();
        }
    }

    private static List<String> remainingIds() throws SQLException {
        try (var c = DriverManager.getConnection(URL, "sa", "");
             var st = c.createStatement();
             var rs = st.executeQuery("SELECT id FROM magpie_outbox_message ORDER BY created_at ASC, id ASC")) {
            var ids = new ArrayList<String>();
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
            return ids;
        }
    }

    private List<String> sentIds() {
        return this.producer.sent.stream().map(MessageRecord::getId).toList();
    }

    private static void createSabotageDeleteTrigger() throws SQLException {
        try (var c = DriverManager.getConnection(URL, "sa", ""); var st = c.createStatement()) {
            st.execute("CREATE TRIGGER sabotage_delete BEFORE DELETE ON magpie_outbox_message"
                    + " FOR EACH ROW CALL \"" + SabotageDeleteTrigger.class.getName() + "\"");
        }
    }

    private static void dropSabotageDeleteTrigger() throws SQLException {
        try (var c = DriverManager.getConnection(URL, "sa", ""); var st = c.createStatement()) {
            st.execute("DROP TRIGGER IF EXISTS sabotage_delete");
        }
    }

    @Test
    void sendsRowsAndDeletesThem() throws SQLException {
        var createdAt = LocalDateTime.now().minusSeconds(5).truncatedTo(ChronoUnit.MILLIS);
        insertRow("aa000000000000000000000000000001", "bk1", createdAt);
        insertRow("aa000000000000000000000000000002", "bk2", createdAt.plusSeconds(1));
        this.start("best_effort");

        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());

        assertEquals(List.of("aa000000000000000000000000000001", "aa000000000000000000000000000002"),
                this.sentIds());
        var record = this.producer.sent.get(0);
        assertEquals("t.order", record.getType());
        assertEquals("orders", record.getTopic());
        assertEquals("t1", record.getTenantId());
        assertEquals("bk1", record.getBusinessKey());
        assertEquals(Map.of("h", "v"), record.getHeaders());
        assertArrayEquals("{\"x\":1}".getBytes(StandardCharsets.UTF_8), record.getPayload());
        assertEquals(createdAt, record.getEventTime());
    }

    @Test
    void orderedBlocksOnFailureThenRetriesInOrder() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(5);
        insertRow("bb000000000000000000000000000001", "", base);
        insertRow("bb000000000000000000000000000002", "", base.plusSeconds(1));
        insertRow("bb000000000000000000000000000003", "", base.plusSeconds(2));
        failNext(this.producer);
        this.start("ordered");

        // 首轮 r1 失败：队头阻塞，r2/r3 不发送，三行都保留
        await().atMost(TIMEOUT).until(() -> this.producer.sent.size() >= 1);
        assertEquals(List.of("bb000000000000000000000000000001"), this.sentIds());
        assertEquals(3, remainingIds().size());

        // 退避后重试：r1 重发成功，全部按序排空
        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
        assertEquals(List.of("bb000000000000000000000000000001",
                        "bb000000000000000000000000000001",
                        "bb000000000000000000000000000002",
                        "bb000000000000000000000000000003"),
                this.sentIds());
    }

    @Test
    void keyOrderedSkipsOnlyTheFailedKey() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(5);
        insertRow("cc0000000000000000000000000000a1", "A", base);
        insertRow("cc0000000000000000000000000000b1", "B", base.plusSeconds(1));
        insertRow("cc0000000000000000000000000000a2", "A", base.plusSeconds(2));
        failNext(this.producer);
        this.start("key_ordered");

        // 首轮：a1 失败 → 本轮跳过 a2；b1 不受影响发送成功并被删除
        await().atMost(TIMEOUT).until(() -> this.producer.sent.size() >= 2);
        assertEquals(List.of("cc0000000000000000000000000000a1",
                        "cc0000000000000000000000000000b1"),
                this.sentIds());
        assertEquals(List.of("cc0000000000000000000000000000a1",
                        "cc0000000000000000000000000000a2"),
                remainingIds());

        // 退避后重试：a1 先于 a2 排空
        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
        assertEquals(List.of("cc0000000000000000000000000000a1",
                        "cc0000000000000000000000000000b1",
                        "cc0000000000000000000000000000a1",
                        "cc0000000000000000000000000000a2"),
                this.sentIds());
    }

    @Test
    void bestEffortFailureDoesNotBlockOthers() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(5);
        insertRow("dd000000000000000000000000000001", "", base);
        insertRow("dd000000000000000000000000000002", "", base.plusSeconds(1));
        insertRow("dd000000000000000000000000000003", "", base.plusSeconds(2));
        failNext(this.producer);
        this.start("best_effort");

        // 首轮：r1 失败但 r2/r3 照常发送并删除，仅 r1 保留
        await().atMost(TIMEOUT).until(() -> remainingIds().size() == 1);
        assertEquals(List.of("dd000000000000000000000000000001"), remainingIds());
        assertEquals(3, this.producer.sent.size());

        // 退避后 r1 重发成功
        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
    }

    @Test
    void emptyBusinessKeyFallsBackToIdByDefault() throws SQLException {
        insertRow("ee000000000000000000000000000001", "", LocalDateTime.now().minusSeconds(5));
        this.start("best_effort");

        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());

        assertEquals("id:ee000000000000000000000000000001",
                this.producer.sent.get(0).getBusinessKey());
    }

    @Test
    void emptyBusinessKeyPreservedWhenFallbackDisabled() throws SQLException {
        insertRow("ee000000000000000000000000000002", "", LocalDateTime.now().minusSeconds(5));
        this.start("best_effort", Map.of("businessKeyFallbackToId", false));

        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());

        assertEquals("", this.producer.sent.get(0).getBusinessKey());
    }

    @Test
    void deleteFailureRedeliversRowsOnNextPoll() throws SQLException {
        var id = "ff000000000000000000000000000001";
        insertRow(id, "", LocalDateTime.now().minusSeconds(5));
        // 触发器让所有 DELETE 失败：发送成功的行删不掉
        createSabotageDeleteTrigger();
        this.start("best_effort");

        // 行已发送但删除失败 → 留在表内
        await().atMost(TIMEOUT).until(() -> this.producer.sent.size() >= 1);
        assertEquals(List.of(id), remainingIds());

        // 撤掉触发器：退避后整行重投（at-least-once 的重复投递），这次删除成功
        dropSabotageDeleteTrigger();
        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
        assertTrue(this.producer.sent.size() >= 2);
        assertEquals(id, this.producer.sent.get(this.producer.sent.size() - 1).getId());
    }

    @Test
    void fullBatchTriggersImmediateNextPoll() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(5);
        insertRow("gg000000000000000000000000000001", "", base);
        insertRow("gg000000000000000000000000000002", "", base.plusSeconds(1));
        insertRow("gg000000000000000000000000000003", "", base.plusSeconds(2));
        // batchSize=2 且 pollInterval 远大于测试超时：首轮满批后必须立即追批，
        // 否则第 3 行要等 30s 后的下一轮，测试根本等不到
        this.start("best_effort", Map.of("batchSize", 2, "pollInterval", 30_000));

        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
        assertEquals(List.of("gg000000000000000000000000000001",
                        "gg000000000000000000000000000002",
                        "gg000000000000000000000000000003"),
                this.sentIds());
    }

    @Test
    void explicitNullOrEmptyUrlIsRejected() {
        // url 显式给 null/空串 → 拒绝；key 缺省不填则回落内置默认地址（不会抛异常）
        var withNull = new HashMap<String, Object>();
        withNull.put("url", null);
        assertThrows(IllegalArgumentException.class,
                () -> new MySqlPollSourceConnector(this.producer, "src-null", withNull));
        assertThrows(IllegalArgumentException.class,
                () -> new MySqlPollSourceConnector(this.producer, "src-empty", Map.of("url", "")));
        assertDoesNotThrow(() -> new MySqlPollSourceConnector(this.producer, "src-default", Map.of()));
    }

    @Test
    void sendTimeoutIsTreatedAsFailureAndRowIsRetried() throws SQLException {
        var id = "hh000000000000000000000000000001";
        insertRow(id, "", LocalDateTime.now().minusSeconds(5));
        // 第一次发送永不完成：awaitResult 吃满 sendTimeout 预算后判失败
        this.producer.thenReturn(new CompletableFuture<>());
        this.start("best_effort", Map.of("sendTimeout", 300, "retryDelay", 200));

        // 超时判失败（CONTINUE 策略）→ 退避重投，第二次发送成功并删除
        await().atMost(TIMEOUT).until(() -> remainingIds().isEmpty());
        assertTrue(this.producer.sent.size() >= 2);
        this.producer.sent.forEach(r -> assertEquals(id, r.getId()));
    }

}
