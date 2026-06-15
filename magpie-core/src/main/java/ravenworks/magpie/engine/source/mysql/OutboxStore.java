package ravenworks.magpie.engine.source.mysql;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public class OutboxStore {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {

    };

    private final String name;
    private final String tableName;
    private final String url;
    private final String username;
    private final String password;

    private Connection connection;

    public OutboxStore(@NonNull String name,
                       @NonNull String tableName,
                       @NonNull String url,
                       @NonNull String username,
                       @NonNull String password) {
        this.name = name;
        this.tableName = tableName;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public boolean ensureConnection() {
        if (this.connection != null) {
            try {
                if (!this.connection.isClosed() && this.connection.isValid(2)) {
                    return true;
                }
            } catch (SQLException e) {
                log.warn("Connection check failed for source '{}'", this.name, e);
            }
            close();
        }
        try {
            this.connection = DriverManager.getConnection(this.url, this.username, this.password);
            log.info("Connected to MySQL for source '{}'", this.name);
            return true;
        } catch (SQLException e) {
            log.error("Failed to connect to MySQL for source '{}': {}", this.name, e.getMessage());
            return false;
        }
    }

    public List<OutboxRecord> queryBatch(int batchSize) {
        List<OutboxRecord> records = new ArrayList<>();
        String sql = "SELECT `id`, `type`, `event_time`, `topic`, `tenant_id`, `business_key`, `headers`, `payload` FROM "
                + this.tableName + " ORDER BY `id` ASC, `event_time` ASC LIMIT ?";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            ps.setInt(1, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    var record = new OutboxRecord();
                    record.setId(rs.getString("id"));
                    record.setType(rs.getString("type"));
                    Timestamp ts = rs.getTimestamp("event_time");
                    record.setEventTime(ts != null ? ts.toLocalDateTime() : null);
                    record.setTopic(rs.getString("topic"));
                    record.setTenantId(rs.getString("tenant_id"));
                    record.setBusinessKey(rs.getString("business_key"));
                    String headersJson = rs.getString("headers");
                    record.setHeaders(JsonUtils.decode(headersJson, HEADERS_TYPE));
                    record.setPayload(rs.getString("payload"));
                    records.add(record);
                }
            }
        } catch (SQLException e) {
            log.error("Query batch failed for source '{}'", this.name, e);
            this.connection = null;
        }
        return records;
    }

    public void deleteBatch(@NonNull List<String> ids) {
        if (ids.isEmpty()) {
            return;
        }
        String placeholders = ids.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));
        String sql = "DELETE FROM " + this.tableName + " WHERE `id` IN (" + placeholders + ")";
        try (PreparedStatement ps = this.connection.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Delete batch failed for source '{}'", this.name, e);
            this.connection = null;
            throw new RuntimeException("Delete batch failed for source '" + this.name + "'", e);
        }
    }

    public void close() {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                log.warn("Failed to close connection for source '{}'", this.name, e);
            }
            this.connection = null;
        }
    }

}
