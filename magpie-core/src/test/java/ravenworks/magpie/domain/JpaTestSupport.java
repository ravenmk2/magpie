package ravenworks.magpie.domain;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import ravenworks.magpie.domain.converter.JsonMapConverter;
import ravenworks.magpie.domain.converter.StringMapConverter;
import ravenworks.magpie.domain.entity.*;
import ravenworks.magpie.domain.repository.*;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;


/**
 * H2-backed test support for production code that depends on the Spring Data JPA
 * repository interfaces in {@link ravenworks.magpie.domain.repository}.
 *
 * <p>magpie-core has no JPA provider on its test classpath (hibernate-core is only
 * pulled into magpie-server via spring-boot-starter-data-jpa), so a real
 * EntityManagerFactory / JpaRepositoryFactory cannot be bootstrapped here without
 * changing pom.xml. Instead this helper creates a plain H2 in-memory database whose
 * schema mirrors docs/database/schema.sql and hands out hand-written JDBC
 * implementations of the repository interfaces — following the project's
 * hand-written-fake test convention (no Mockito). JSON columns are serialized through
 * the same {@link JsonMapConverter} / {@link StringMapConverter} used by the entities,
 * so converter behavior is covered as well.
 *
 * <p>Usage:
 * <pre>{@code
 * try (JpaTestSupport support = JpaTestSupport.create("my-test-db")) {
 *     TopicRepository topics = support.repository(TopicRepository.class);
 *     ...
 * }
 * }</pre>
 *
 * <p>Give every test class its own dbName so no state is shared between classes.
 * Supported repository methods: findById / existsById / findAll / findAllById /
 * save / saveAll / deleteById / count (all repositories), plus the custom queries
 * of {@link ConsumerOffsetRepository} (updateOffset) and {@link RetryMessageRepository}
 * (findDistinctBusinessKeysByConsumer, findByConsumerOrderByOffsetAsc,
 * findByConsumerAndRetryAtBeforeOrderByOffsetAsc) — enough for OffsetTrackerImpl,
 * the registries and RetryMessageStoreImpl. Anything else throws
 * UnsupportedOperationException.
 */
public final class JpaTestSupport implements AutoCloseable {

    private static final JsonMapConverter JSON_MAP = new JsonMapConverter();
    private static final StringMapConverter STRING_MAP = new StringMapConverter();

    private static final String[] DDL_STATEMENTS = {
            "DROP TABLE IF EXISTS magpie_topic",
            "CREATE TABLE magpie_topic ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY, "
                    + "name VARCHAR(128) NOT NULL DEFAULT '', "
                    + "title VARCHAR(128) NOT NULL DEFAULT '', "
                    + "partitions INT NOT NULL DEFAULT 0, "
                    + "properties CLOB NOT NULL, "
                    + "version INT NOT NULL DEFAULT 0, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",

            "DROP TABLE IF EXISTS magpie_source",
            "CREATE TABLE magpie_source ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY, "
                    + "type VARCHAR(32) NOT NULL DEFAULT '', "
                    + "name VARCHAR(128) NOT NULL DEFAULT '', "
                    + "title VARCHAR(128) NOT NULL DEFAULT '', "
                    + "is_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                    + "properties CLOB NOT NULL, "
                    + "version INT NOT NULL DEFAULT 0, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",

            "DROP TABLE IF EXISTS magpie_target",
            "CREATE TABLE magpie_target ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY, "
                    + "type VARCHAR(32) NOT NULL DEFAULT '', "
                    + "name VARCHAR(128) NOT NULL DEFAULT '', "
                    + "title VARCHAR(128) NOT NULL DEFAULT '', "
                    + "topic VARCHAR(128) NOT NULL DEFAULT '', "
                    + "is_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                    + "properties CLOB NOT NULL, "
                    + "version INT NOT NULL DEFAULT 0, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",

            "DROP TABLE IF EXISTS magpie_consumer_offset",
            "CREATE TABLE magpie_consumer_offset ("
                    + "id VARCHAR(128) NOT NULL PRIMARY KEY, "
                    + "name VARCHAR(128) NOT NULL DEFAULT '', "
                    + "\"partition\" INT NOT NULL DEFAULT 0, "
                    + "\"offset\" BIGINT NOT NULL DEFAULT 0, "
                    + "version INT NOT NULL DEFAULT 0, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",

            "DROP TABLE IF EXISTS magpie_message_log",
            "CREATE TABLE magpie_message_log ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY, "
                    + "message_id VARCHAR(32) NOT NULL, "
                    + "type VARCHAR(128) NOT NULL DEFAULT '', "
                    + "event_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "topic VARCHAR(128) NOT NULL DEFAULT '', "
                    + "tenant_id VARCHAR(64) NOT NULL DEFAULT '', "
                    + "business_key VARCHAR(128) NOT NULL DEFAULT '', "
                    + "headers CLOB NOT NULL, "
                    + "payload CLOB NOT NULL, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",

            "DROP TABLE IF EXISTS magpie_retry_message",
            "CREATE TABLE magpie_retry_message ("
                    + "id VARCHAR(32) NOT NULL PRIMARY KEY, "
                    + "consumer VARCHAR(128) NOT NULL DEFAULT '', "
                    + "log_id VARCHAR(32) NOT NULL DEFAULT '', "
                    + "\"offset\" BIGINT NOT NULL DEFAULT -1, "
                    + "attempts INT NOT NULL DEFAULT 0, "
                    + "retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "business_key VARCHAR(128) NOT NULL DEFAULT '', "
                    + "version INT NOT NULL DEFAULT 0, "
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    };

    private final JdbcTemplate jdbc;
    private final Map<Class<?>, EntityStore<?>> storesByRepository;

    private JpaTestSupport(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.storesByRepository = Map.of(
                TopicRepository.class, topicStore(),
                SourceRepository.class, sourceStore(),
                TargetRepository.class, targetStore(),
                ConsumerOffsetRepository.class, consumerOffsetStore(),
                MessageLogRepository.class, messageLogStore(),
                RetryMessageRepository.class, retryMessageStore());
    }

    /**
     * Creates a fresh H2 in-memory database (jdbc:h2:mem:[dbName]) with the full
     * magpie schema. Any existing tables in a database of the same name are dropped
     * first, so each call starts from a clean slate.
     */
    public static JpaTestSupport create(String dbName) {
        var dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
        var jdbc = new JdbcTemplate(dataSource);
        for (String ddl : DDL_STATEMENTS) {
            jdbc.execute(ddl);
        }
        return new JpaTestSupport(jdbc);
    }

    public DataSource dataSource() {
        return this.jdbc.getDataSource();
    }

    /**
     * Returns a JDBC-backed implementation of the given Spring Data repository
     * interface. See the class javadoc for the supported method set.
     */
    @SuppressWarnings("unchecked")
    public <T> T repository(Class<T> repositoryInterface) {
        EntityStore<?> store = this.storesByRepository.get(repositoryInterface);
        if (store == null) {
            throw new UnsupportedOperationException(
                    "JpaTestSupport has no JDBC-backed implementation for " + repositoryInterface.getName());
        }
        return (T) Proxy.newProxyInstance(
                repositoryInterface.getClassLoader(),
                new Class<?>[]{repositoryInterface},
                new RepositoryHandler(this.jdbc, repositoryInterface, store));
    }

    @Override
    public void close() {
        this.jdbc.execute("DROP ALL OBJECTS");
    }

    private static final class RepositoryHandler implements InvocationHandler {

        private final JdbcTemplate jdbc;
        private final Class<?> repositoryInterface;
        private final EntityStore<Object> store;

        @SuppressWarnings("unchecked")
        private RepositoryHandler(JdbcTemplate jdbc, Class<?> repositoryInterface, EntityStore<?> store) {
            this.jdbc = jdbc;
            this.repositoryInterface = repositoryInterface;
            this.store = (EntityStore<Object>) store;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "toString" -> "JpaTestSupport proxy for " + this.repositoryInterface.getSimpleName();
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> throw new UnsupportedOperationException(method.toString());
                };
            }
            return switch (method.getName()) {
                case "findById" -> this.store.findById(this.jdbc, (String) args[0]);
                case "existsById" -> this.store.findById(this.jdbc, (String) args[0]).isPresent();
                case "findAll" -> this.store.findAll(this.jdbc);
                case "findAllById" -> this.store.findAllById(this.jdbc, (Iterable<String>) args[0]);
                case "save" -> this.store.save(this.jdbc, args[0]);
                case "saveAll" -> {
                    for (Object entity : (Iterable<?>) args[0]) {
                        this.store.save(this.jdbc, entity);
                    }
                    yield args[0];
                }
                case "deleteById" -> {
                    this.store.deleteById(this.jdbc, (String) args[0]);
                    yield null;
                }
                case "count" -> this.store.count(this.jdbc);
                // ConsumerOffsetRepository — mirrors the native @Query in production
                case "updateOffset" -> this.jdbc.update(
                        "UPDATE magpie_consumer_offset SET \"offset\" = ?, version = version + 1 WHERE id = ?",
                        args[1], args[0]);
                // RetryMessageRepository custom queries
                case "findDistinctBusinessKeysByConsumer" -> new LinkedHashSet<>(this.jdbc.queryForList(
                        "SELECT DISTINCT business_key FROM magpie_retry_message WHERE consumer = ?",
                        String.class, args[0]));
                case "findByConsumerOrderByOffsetAsc" -> {
                    var pageable = (Pageable) args[1];
                    yield this.store.queryWhere(this.jdbc,
                            "WHERE consumer = ? ORDER BY \"offset\" ASC LIMIT ? OFFSET ?",
                            args[0], pageable.getPageSize(), pageable.getOffset());
                }
                case "findByConsumerAndRetryAtBeforeOrderByOffsetAsc" -> {
                    var pageable = (Pageable) args[2];
                    yield this.store.queryWhere(this.jdbc,
                            "WHERE consumer = ? AND retry_at < ? ORDER BY \"offset\" ASC LIMIT ? OFFSET ?",
                            args[0], args[1], pageable.getPageSize(), pageable.getOffset());
                }
                default -> throw new UnsupportedOperationException(
                        "JpaTestSupport does not implement " + this.repositoryInterface.getSimpleName()
                                + "." + method.getName() + " — extend JpaTestSupport if a test needs it");
            };
        }

    }


    private static final class EntityStore<E> {

        private final String table;
        private final String columns;
        private final String insertSql;
        private final String updateSql;
        private final Function<E, Object[]> insertArgs;
        private final Function<E, Object[]> updateArgs;
        private final Function<E, String> idOf;
        private final RowMapper<E> rowMapper;

        private EntityStore(String table, String columns,
                            String insertSql, String updateSql,
                            Function<E, Object[]> insertArgs, Function<E, Object[]> updateArgs,
                            Function<E, String> idOf, RowMapper<E> rowMapper) {
            this.table = table;
            this.columns = columns;
            this.insertSql = insertSql;
            this.updateSql = updateSql;
            this.insertArgs = insertArgs;
            this.updateArgs = updateArgs;
            this.idOf = idOf;
            this.rowMapper = rowMapper;
        }

        Optional<E> findById(JdbcTemplate jdbc, String id) {
            var rows = jdbc.query(
                    "SELECT " + this.columns + " FROM " + this.table + " WHERE id = ?", this.rowMapper, id);
            return rows.stream().findFirst();
        }

        List<E> findAll(JdbcTemplate jdbc) {
            return jdbc.query("SELECT " + this.columns + " FROM " + this.table, this.rowMapper);
        }

        List<E> findAllById(JdbcTemplate jdbc, Iterable<String> ids) {
            var idList = new ArrayList<String>();
            ids.forEach(idList::add);
            if (idList.isEmpty()) {
                return List.of();
            }
            String placeholders = String.join(", ", Collections.nCopies(idList.size(), "?"));
            return jdbc.query("SELECT " + this.columns + " FROM " + this.table
                    + " WHERE id IN (" + placeholders + ")", this.rowMapper, idList.toArray());
        }

        E save(JdbcTemplate jdbc, E entity) {
            if (findById(jdbc, this.idOf.apply(entity)).isPresent()) {
                jdbc.update(this.updateSql, this.updateArgs.apply(entity));
            } else {
                jdbc.update(this.insertSql, this.insertArgs.apply(entity));
            }
            return entity;
        }

        void deleteById(JdbcTemplate jdbc, String id) {
            jdbc.update("DELETE FROM " + this.table + " WHERE id = ?", id);
        }

        long count(JdbcTemplate jdbc) {
            Long count = jdbc.queryForObject("SELECT COUNT(*) FROM " + this.table, Long.class);
            return count != null ? count : 0L;
        }

        List<E> queryWhere(JdbcTemplate jdbc, String whereAndOrder, Object... args) {
            return jdbc.query("SELECT " + this.columns + " FROM " + this.table + " " + whereAndOrder,
                    this.rowMapper, args);
        }

    }

    private static EntityStore<TopicEntity> topicStore() {
        return new EntityStore<>(
                "magpie_topic",
                "id, name, title, partitions, properties, version, created_at, updated_at",
                "INSERT INTO magpie_topic (id, name, title, partitions, properties, version)"
                        + " VALUES (?, ?, ?, ?, ?, 0)",
                "UPDATE magpie_topic SET name = ?, title = ?, partitions = ?, properties = ?,"
                        + " version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                e -> new Object[]{e.getId(), e.getName(), e.getTitle(), e.getPartitions(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties())},
                e -> new Object[]{e.getName(), e.getTitle(), e.getPartitions(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties()), e.getId()},
                TopicEntity::getId,
                (rs, rowNum) -> {
                    var entity = new TopicEntity();
                    entity.setId(rs.getString("id"));
                    entity.setName(rs.getString("name"));
                    entity.setTitle(rs.getString("title"));
                    entity.setPartitions(rs.getInt("partitions"));
                    entity.setProperties(JSON_MAP.convertToEntityAttribute(rs.getString("properties")));
                    entity.setVersion(rs.getInt("version"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    entity.setUpdatedAt(rs.getObject("updated_at", LocalDateTime.class));
                    return entity;
                });
    }

    private static EntityStore<SourceEntity> sourceStore() {
        return new EntityStore<>(
                "magpie_source",
                "id, type, name, title, is_enabled, properties, version, created_at, updated_at",
                "INSERT INTO magpie_source (id, type, name, title, is_enabled, properties, version)"
                        + " VALUES (?, ?, ?, ?, ?, ?, 0)",
                "UPDATE magpie_source SET type = ?, name = ?, title = ?, is_enabled = ?, properties = ?,"
                        + " version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                e -> new Object[]{e.getId(), e.getType(), e.getName(), e.getTitle(), e.isEnabled(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties())},
                e -> new Object[]{e.getType(), e.getName(), e.getTitle(), e.isEnabled(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties()), e.getId()},
                SourceEntity::getId,
                (rs, rowNum) -> {
                    var entity = new SourceEntity();
                    entity.setId(rs.getString("id"));
                    entity.setType(rs.getString("type"));
                    entity.setName(rs.getString("name"));
                    entity.setTitle(rs.getString("title"));
                    entity.setEnabled(rs.getBoolean("is_enabled"));
                    entity.setProperties(JSON_MAP.convertToEntityAttribute(rs.getString("properties")));
                    entity.setVersion(rs.getInt("version"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    entity.setUpdatedAt(rs.getObject("updated_at", LocalDateTime.class));
                    return entity;
                });
    }

    private static EntityStore<TargetEntity> targetStore() {
        return new EntityStore<>(
                "magpie_target",
                "id, type, name, title, topic, is_enabled, properties, version, created_at, updated_at",
                "INSERT INTO magpie_target (id, type, name, title, topic, is_enabled, properties, version)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                "UPDATE magpie_target SET type = ?, name = ?, title = ?, topic = ?, is_enabled = ?,"
                        + " properties = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                e -> new Object[]{e.getId(), e.getType(), e.getName(), e.getTitle(), e.getTopic(), e.isEnabled(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties())},
                e -> new Object[]{e.getType(), e.getName(), e.getTitle(), e.getTopic(), e.isEnabled(),
                        JSON_MAP.convertToDatabaseColumn(e.getProperties()), e.getId()},
                TargetEntity::getId,
                (rs, rowNum) -> {
                    var entity = new TargetEntity();
                    entity.setId(rs.getString("id"));
                    entity.setType(rs.getString("type"));
                    entity.setName(rs.getString("name"));
                    entity.setTitle(rs.getString("title"));
                    entity.setTopic(rs.getString("topic"));
                    entity.setEnabled(rs.getBoolean("is_enabled"));
                    entity.setProperties(JSON_MAP.convertToEntityAttribute(rs.getString("properties")));
                    entity.setVersion(rs.getInt("version"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    entity.setUpdatedAt(rs.getObject("updated_at", LocalDateTime.class));
                    return entity;
                });
    }

    private static EntityStore<ConsumerOffsetEntity> consumerOffsetStore() {
        return new EntityStore<>(
                "magpie_consumer_offset",
                "id, name, \"partition\", \"offset\", version, created_at, updated_at",
                "INSERT INTO magpie_consumer_offset (id, name, \"partition\", \"offset\", version)"
                        + " VALUES (?, ?, ?, ?, 0)",
                "UPDATE magpie_consumer_offset SET name = ?, \"partition\" = ?, \"offset\" = ?,"
                        + " version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                e -> new Object[]{e.getId(), e.getName(), e.getPartition(), e.getOffset()},
                e -> new Object[]{e.getName(), e.getPartition(), e.getOffset(), e.getId()},
                ConsumerOffsetEntity::getId,
                (rs, rowNum) -> {
                    var entity = new ConsumerOffsetEntity();
                    entity.setId(rs.getString("id"));
                    entity.setName(rs.getString("name"));
                    entity.setPartition(rs.getInt("partition"));
                    entity.setOffset(rs.getLong("offset"));
                    entity.setVersion(rs.getInt("version"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    entity.setUpdatedAt(rs.getObject("updated_at", LocalDateTime.class));
                    return entity;
                });
    }

    private static EntityStore<MessageLogEntity> messageLogStore() {
        return new EntityStore<>(
                "magpie_message_log",
                "id, message_id, type, event_time, topic, tenant_id, business_key, headers, payload, created_at",
                "INSERT INTO magpie_message_log"
                        + " (id, message_id, type, event_time, topic, tenant_id, business_key, headers, payload)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "UPDATE magpie_message_log SET message_id = ?, type = ?, event_time = ?, topic = ?,"
                        + " tenant_id = ?, business_key = ?, headers = ?, payload = ? WHERE id = ?",
                e -> new Object[]{e.getId(), e.getMessageId(), e.getType(), e.getEventTime(), e.getTopic(),
                        e.getTenantId(), e.getBusinessKey(),
                        STRING_MAP.convertToDatabaseColumn(e.getHeaders()), e.getPayload()},
                e -> new Object[]{e.getMessageId(), e.getType(), e.getEventTime(), e.getTopic(),
                        e.getTenantId(), e.getBusinessKey(),
                        STRING_MAP.convertToDatabaseColumn(e.getHeaders()), e.getPayload(), e.getId()},
                MessageLogEntity::getId,
                (rs, rowNum) -> {
                    var entity = new MessageLogEntity();
                    entity.setId(rs.getString("id"));
                    entity.setMessageId(rs.getString("message_id"));
                    entity.setType(rs.getString("type"));
                    entity.setEventTime(rs.getObject("event_time", LocalDateTime.class));
                    entity.setTopic(rs.getString("topic"));
                    entity.setTenantId(rs.getString("tenant_id"));
                    entity.setBusinessKey(rs.getString("business_key"));
                    entity.setHeaders(STRING_MAP.convertToEntityAttribute(rs.getString("headers")));
                    entity.setPayload(rs.getString("payload"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    return entity;
                });
    }

    private static EntityStore<RetryMessageEntity> retryMessageStore() {
        return new EntityStore<>(
                "magpie_retry_message",
                "id, consumer, log_id, \"offset\", attempts, retry_at, business_key,"
                        + " version, created_at, updated_at",
                "INSERT INTO magpie_retry_message"
                        + " (id, consumer, log_id, \"offset\", attempts, retry_at, business_key, version)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                "UPDATE magpie_retry_message SET consumer = ?, log_id = ?, \"offset\" = ?, attempts = ?,"
                        + " retry_at = ?, business_key = ?, version = version + 1,"
                        + " updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                e -> new Object[]{e.getId(), e.getConsumer(), e.getLogId(), e.getOffset(), e.getAttempts(),
                        e.getRetryAt(), e.getBusinessKey()},
                e -> new Object[]{e.getConsumer(), e.getLogId(), e.getOffset(), e.getAttempts(),
                        e.getRetryAt(), e.getBusinessKey(), e.getId()},
                RetryMessageEntity::getId,
                (rs, rowNum) -> {
                    var entity = new RetryMessageEntity();
                    entity.setId(rs.getString("id"));
                    entity.setConsumer(rs.getString("consumer"));
                    entity.setLogId(rs.getString("log_id"));
                    entity.setOffset(rs.getLong("offset"));
                    entity.setAttempts(rs.getInt("attempts"));
                    entity.setRetryAt(rs.getObject("retry_at", LocalDateTime.class));
                    entity.setBusinessKey(rs.getString("business_key"));
                    entity.setVersion(rs.getInt("version"));
                    entity.setCreatedAt(rs.getObject("created_at", LocalDateTime.class));
                    entity.setUpdatedAt(rs.getObject("updated_at", LocalDateTime.class));
                    return entity;
                });
    }

}
