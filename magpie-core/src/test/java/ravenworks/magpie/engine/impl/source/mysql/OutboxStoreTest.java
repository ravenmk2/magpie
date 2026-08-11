package ravenworks.magpie.engine.impl.source.mysql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class OutboxStoreTest {

    private static final String URL = "jdbc:h2:mem:outbox_store;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String USER = "sa";
    private static final String PASSWORD = "";

    private OutboxStore store;

    @BeforeEach
    void setUp() throws SQLException {
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD); var st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS magpie_outbox_message");
            st.execute("""
                    CREATE TABLE magpie_outbox_message (
                        id CHAR(32) NOT NULL PRIMARY KEY,
                        type VARCHAR(128) NOT NULL DEFAULT '',
                        event_time DATETIME(3) NOT NULL,
                        topic VARCHAR(128) NOT NULL DEFAULT '',
                        tenant_id VARCHAR(64) NOT NULL DEFAULT '',
                        business_key VARCHAR(128) NOT NULL DEFAULT '',
                        headers VARCHAR(2048),
                        payload MEDIUMTEXT NOT NULL,
                        created_at DATETIME(3) NOT NULL
                    )
                    """);
        }
        this.store = new OutboxStore(URL, USER, PASSWORD, "magpie_outbox_message", 500);
    }

    private static void insertRow(String id, String businessKey, LocalDateTime createdAt) throws SQLException {
        insertRow(id, businessKey, "{\"h\":\"v\"}", createdAt);
    }

    private static void insertRow(String id, String businessKey, String headers, LocalDateTime createdAt)
            throws SQLException {
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD);
             var ps = c.prepareStatement("""
                     INSERT INTO magpie_outbox_message
                         (id, type, event_time, topic, tenant_id, business_key, headers, payload, created_at)
                     VALUES (?, 't.order', ?, 'orders', 't1', ?, ?, ?, ?)
                     """)) {
            ps.setString(1, id);
            ps.setTimestamp(2, Timestamp.valueOf(createdAt));
            ps.setString(3, businessKey);
            ps.setString(4, headers);
            ps.setString(5, "{\"x\":1}");
            ps.setTimestamp(6, Timestamp.valueOf(createdAt));
            ps.executeUpdate();
        }
    }

    private static int count() throws SQLException {
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD);
             var st = c.createStatement();
             var rs = st.executeQuery("SELECT COUNT(*) FROM magpie_outbox_message")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void queryBatchOrdersByCreatedAtThenIdAndFiltersRecentRows() throws SQLException {
        var now = LocalDateTime.now();
        // readLag 窗口外（太近）的行不应被读到
        insertRow("cc0000000000000000000000000000c3", "", now);
        // 同一 created_at 按 id 升序
        insertRow("cc0000000000000000000000000000c2", "", now.minusSeconds(10));
        insertRow("cc0000000000000000000000000000c1", "", now.minusSeconds(10));
        insertRow("cc0000000000000000000000000000c0", "", now.minusSeconds(20));

        var batch = this.store.queryBatch(100);

        assertEquals(List.of("cc0000000000000000000000000000c0",
                        "cc0000000000000000000000000000c1",
                        "cc0000000000000000000000000000c2"),
                batch.stream().map(OutboxRecord::getId).toList());
    }

    @Test
    void queryBatchMapsRowFields() throws SQLException {
        // DATETIME(3) 精度为毫秒
        var createdAt = LocalDateTime.now().minusSeconds(10).truncatedTo(ChronoUnit.MILLIS);
        insertRow("cc0000000000000000000000000000d0", "bk1", createdAt);

        var batch = this.store.queryBatch(100);

        assertEquals(1, batch.size());
        var record = batch.get(0);
        assertEquals("cc0000000000000000000000000000d0", record.getId());
        assertEquals("t.order", record.getType());
        assertEquals("orders", record.getTopic());
        assertEquals("t1", record.getTenantId());
        assertEquals("bk1", record.getBusinessKey());
        assertEquals(Map.of("h", "v"), record.getHeaders());
        assertEquals("{\"x\":1}", record.getPayload());
        assertEquals(createdAt, record.getCreatedAt());
        assertEquals(createdAt, record.getEventTime());
    }

    @Test
    void queryBatchRespectsLimit() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(10);
        for (int i = 0; i < 3; i++) {
            insertRow(String.format("cc0000000000000000000000000000e%d", i), "", base.plusSeconds(i));
        }

        assertEquals(2, this.store.queryBatch(2).size());
    }

    @Test
    void deleteBatchDeletesOnlyGivenIds() throws SQLException {
        var base = LocalDateTime.now().minusSeconds(10);
        insertRow("cc0000000000000000000000000000f0", "", base);
        insertRow("cc0000000000000000000000000000f1", "", base.plusSeconds(1));
        insertRow("cc0000000000000000000000000000f2", "", base.plusSeconds(2));

        this.store.deleteBatch(List.of("cc0000000000000000000000000000f0",
                "cc0000000000000000000000000000f2"));

        assertEquals(List.of("cc0000000000000000000000000000f1"),
                this.store.queryBatch(100).stream().map(OutboxRecord::getId).toList());
    }

    @Test
    void illegalTableNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new OutboxStore(URL, USER, PASSWORD, "x;DROP TABLE y", 500));
        assertThrows(IllegalArgumentException.class,
                () -> new OutboxStore(URL, USER, PASSWORD, "has space", 500));
    }

    @Test
    void connectionIsRecreatedAfterLoss() throws SQLException {
        insertRow("cc0000000000000000000000000000aa", "", LocalDateTime.now().minusSeconds(10));
        assertEquals(1, this.store.queryBatch(100).size());

        // 连接失效后下一次操作前自动重建
        this.store.close();

        assertEquals(1, this.store.queryBatch(100).size());
    }

    @Test
    void staleConnectionIsDetectedLazilyAndRetried() throws Exception {
        insertRow("cc0000000000000000000000000000ab", "", LocalDateTime.now().minusSeconds(10));
        assertEquals(1, this.store.queryBatch(100).size());

        // 模拟连接被服务端单方面断开（store 不知情，没有 isValid 探测）
        var field = OutboxStore.class.getDeclaredField("connection");
        field.setAccessible(true);
        ((java.sql.Connection) field.get(this.store)).close();

        // 首次执行失败 → 重连并重试一次后成功
        assertEquals(1, this.store.queryBatch(100).size());
    }

    @Test
    void nonTransientErrorIsRethrownWithoutRetry() throws SQLException {
        // 先建立连接，再删表：表不存在（42S02）是非瞬态错误
        this.store.queryBatch(100);
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD); var st = c.createStatement()) {
            st.execute("DROP TABLE magpie_outbox_message");
        }

        // 只有连接类/瞬态错误才当场重试一次，语法/权限类直接上抛
        var e = assertThrows(SQLException.class, () -> this.store.queryBatch(100));
        assertTrue(e.getSQLState().startsWith("42"));
    }

    @Test
    void deleteBatchWithEmptyIdsDoesNothing() throws SQLException {
        // 表都不存在也不报错：空列表直接返回，不执行任何 SQL
        try (var c = DriverManager.getConnection(URL, USER, PASSWORD); var st = c.createStatement()) {
            st.execute("DROP TABLE magpie_outbox_message");
        }

        this.store.deleteBatch(List.of());
    }

    @Test
    void nullHeadersAreMappedToEmptyMap() throws SQLException {
        insertRow("cc0000000000000000000000000000ba", "bk1", null, LocalDateTime.now().minusSeconds(10));

        var batch = this.store.queryBatch(100);

        assertEquals(1, batch.size());
        assertEquals(Map.of(), batch.get(0).getHeaders());
    }

}
