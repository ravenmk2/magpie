package ravenworks.magpie.engine.impl.source.mysql;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * outbox 表的最薄 JDBC 封装：DriverManager 单连接，仅 WorkLoop 线程使用，不支持并发。
 * 连接失效后下一次操作前自动重建（自动重连）。
 *
 * @author Raven
 */
@Slf4j
class OutboxStore implements AutoCloseable {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {

    };
    private static final String TABLE_NAME_PATTERN = "[a-zA-Z0-9_]+";

    private final String url;
    private final String username;
    private final String password;
    private final int readLag;
    private final String tableName;
    private final String querySql;

    private Connection connection;

    OutboxStore(@NonNull String url,
                String username,
                String password,
                @NonNull String tableName,
                int readLag) {
        if (!tableName.matches(TABLE_NAME_PATTERN)) {
            throw new IllegalArgumentException("Illegal outbox table name: " + tableName);
        }
        this.url = url;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.readLag = readLag;
        this.querySql = "SELECT id, type, event_time, topic, tenant_id, business_key, headers, payload, created_at"
                + " FROM " + tableName
                + " WHERE created_at < ? ORDER BY created_at ASC, id ASC LIMIT ?";
    }

    /**
     * 按 (created_at, id) 升序取一批已稳定可见的行（created_at 早于 now - readLag）。
     * 不使用单调游标：迟提交的行下轮仍会扫到，保证不丢。
     */
    List<OutboxRecord> queryBatch(int limit) throws SQLException {
        // 时间界在应用侧算：业务事务从 INSERT 到 COMMIT 的时长必须小于 readLag
        var bound = Timestamp.valueOf(LocalDateTime.now().minusNanos(this.readLag * 1_000_000L));
        return this.withReconnect(() -> {
            try (var ps = this.connection.prepareStatement(this.querySql)) {
                ps.setTimestamp(1, bound);
                ps.setInt(2, limit);
                try (var rs = ps.executeQuery()) {
                    var records = new ArrayList<OutboxRecord>();
                    while (rs.next()) {
                        records.add(mapRow(rs));
                    }
                    return records;
                }
            }
        });
    }

    /**
     * 删除已成功发送的行。删除失败抛异常，已发送的行下轮重投（at-least-once）。
     * DELETE 幂等，重连后重试安全。
     */
    void deleteBatch(@NonNull List<String> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        var placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        this.withReconnect(() -> {
            try (var ps = this.connection.prepareStatement(
                    "DELETE FROM " + this.tableName + " WHERE id IN (" + placeholders + ")")) {
                for (int i = 0; i < ids.size(); i++) {
                    ps.setString(i + 1, ids.get(i));
                }
                ps.executeUpdate();
                return null;
            }
        });
    }

    @Override
    public void close() {
        this.invalidate();
    }

    /**
     * 惰性检测连接失效：不做 isValid 探测（省一次 DB 往返）。
     * 操作抛 SQLException 时一律先失效连接（下轮必重建），
     * 但只有连接类/瞬态错误才当场重试一次；语法、权限等非瞬态错误直接上抛，不做无谓重试。
     */
    private <T> T withReconnect(SQLOperation<T> operation) throws SQLException {
        this.ensureConnection();
        try {
            return operation.execute();
        } catch (SQLException e) {
            // isClosed 先于 invalidate 检查：客户端侧已关闭的连接任何驱动都能识别
            var connectionFailure = isConnectionFailure(e) || isClosedQuietly(this.connection);
            this.invalidate();
            if (!connectionFailure) {
                throw e;
            }
            log.debug("Outbox connection failed, reconnecting and retrying once", e);
            this.ensureConnection();
            return operation.execute();
        }
    }

    /**
     * 是否连接类/瞬态错误：JDBC 异常层次 + SQLState 08xxx（SQL 标准连接异常类码）双保险。
     */
    private static boolean isConnectionFailure(SQLException e) {
        if (e instanceof SQLTransientException
                || e instanceof SQLRecoverableException
                || e instanceof SQLNonTransientConnectionException) {
            return true;
        }
        var state = e.getSQLState();
        return state != null && state.startsWith("08");
    }

    private static boolean isClosedQuietly(Connection c) {
        try {
            return c == null || c.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    private void ensureConnection() throws SQLException {
        if (this.connection == null) {
            log.debug("Opening outbox connection to {}", this.url);
            this.connection = DriverManager.getConnection(this.url, this.username, this.password);
        }
    }

    private void invalidate() {
        var c = this.connection;
        this.connection = null;
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
                log.debug("Failed to close outbox connection", e);
            }
        }
    }

    private static OutboxRecord mapRow(ResultSet rs) throws SQLException {
        var record = new OutboxRecord();
        record.setId(rs.getString("id"));
        record.setType(rs.getString("type"));
        record.setEventTime(rs.getTimestamp("event_time").toLocalDateTime());
        record.setTopic(rs.getString("topic"));
        record.setTenantId(rs.getString("tenant_id"));
        record.setBusinessKey(rs.getString("business_key"));
        var headers = rs.getString("headers");
        record.setHeaders(headers == null ? Map.of() : JsonUtils.decode(headers, HEADERS_TYPE));
        record.setPayload(rs.getString("payload"));
        record.setCreatedAt(rs.getTimestamp("created_at").toLocalDateTime());
        return record;
    }

    @FunctionalInterface
    private interface SQLOperation<T> {

        T execute() throws SQLException;

    }

}
