package ravenworks.magpie.server;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;
import ravenworks.magpie.server.dto.ErrorResponse;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;


/**
 * 服务端全链路冒烟：Testcontainers 起 MySQL（按 docs/database/schema.sql 建表）与
 * RabbitMQ Stream，@SpringBootTest 随机端口启动完整应用，播种 topic/source/target
 * 后经 POST /api/v1/publish/{source} 发布 CloudEvent，验证消息落到 stream 并被
 * print sink 消费提交 offset。
 *
 * <p>注意：CloudEvent 的 subject 即 topic（stream 名），路由按 subject 精确匹配
 * magpie_topic.name，因此播种的 topic 名、allowedTopics、subject 必须一致。
 *
 * <p>容器采用手工 start 的 singleton 模式（与 magpie-core 的 TestMySql/TestRabbitMq 一致），
 * Coordinator reconcile 节拍 5s（Coordinator.DEFAULT_RESYNC_INTERVAL_MS），
 * 所有等待一律用 Awaitility，不 sleep。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServerE2eIT {

    private static final int STREAM_PORT = 5552;

    private static final Path SCHEMA_SQL = Path.of("..", "docs", "database", "schema.sql");

    /**
     * subject 即 topic：stream 名、allowedTopics、target.topic、CloudEvent subject 统一用它
     */
    private static final String TOPIC = "server-e2e-orders";
    private static final String SOURCE = "server-e2e-http";
    private static final String TARGET = "server-e2e-print";

    private static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    private static final RabbitMQContainer RABBITMQ =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.3.1-management"))
                    .withExposedPorts(STREAM_PORT)
                    .withPluginsEnabled("rabbitmq_stream");

    static {
        MYSQL.start();
        RABBITMQ.start();
        applySchema();
    }

    private static void applySchema() {
        // failsafe 以模块 basedir 为工作目录，../docs/... 相对路径可解析
        try (Connection connection = DriverManager.getConnection(
                MYSQL.getJdbcUrl() + "?useSSL=false&allowPublicKeyRetrieval=true",
                MYSQL.getUsername(), MYSQL.getPassword())) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource(SCHEMA_SQL));
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to apply " + SCHEMA_SQL, e);
        }
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> MYSQL.getJdbcUrl() + "?useSSL=false&allowPublicKeyRetrieval=true");
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "com.mysql.cj.jdbc.Driver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        registry.add("magpie.rabbitmq-stream.uris", () -> "rabbitmq-stream://"
                + RABBITMQ.getAdminUsername() + ":" + RABBITMQ.getAdminPassword()
                + "@" + RABBITMQ.getHost() + ":" + RABBITMQ.getMappedPort(STREAM_PORT) + "/%2f");
    }

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private TestRestTemplate rest;

    private volatile boolean sourceReady;

    @BeforeAll
    void seed() {
        // id 列 CHAR(32)：uuid hex；properties 是 JSON 列 NOT NULL，其余列用 schema 默认值
        this.jdbc.update("INSERT INTO magpie_topic (id, name, partitions, properties) VALUES (?, ?, ?, ?)",
                id32(), TOPIC, 1, "{}");
        this.jdbc.update("INSERT INTO magpie_source (id, type, name, is_enabled, properties) VALUES (?, ?, ?, ?, ?)",
                id32(), "http", SOURCE, 1, "{\"allowedTopics\":[\"" + TOPIC + "\"]}");
        this.jdbc.update("INSERT INTO magpie_target (id, type, name, topic, is_enabled, properties) VALUES (?, ?, ?, ?, ?, ?)",
                id32(), "print", TARGET, TOPIC, 1, "{}");
    }

    @Test
    void structuredCloudEventFlowsToStreamAndSink() {
        awaitSourceReady();
        long offsetBefore = committedOffset();

        ResponseEntity<String> response = postStructured(TOPIC, "{\"orderId\":\"o-1\"}", String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // print sink 消费这条消息后由 OffsetTrackerImpl 提交 offset（值为 last offset + 1），
        // 断言发布后水位严格增大（排除探针流量提交的干扰），超时覆盖 sink 消费与提交延迟
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertTrue(committedOffset() > offsetBefore,
                        "expected committed offset > " + offsetBefore + " for target " + TARGET
                                + ", got " + committedOffset()));
    }

    /**
     * print target 当前已提交的最大 offset；尚无提交记录时视为 -1。
     */
    private long committedOffset() {
        List<Long> offsets = this.jdbc.queryForList(
                "SELECT `offset` FROM magpie_consumer_offset WHERE name = ?", Long.class, TARGET);
        return offsets.stream().mapToLong(Long::longValue).max().orElse(-1);
    }

    @Test
    void binaryModeCloudEventAccepted() {
        awaitSourceReady();

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("ce-specversion", "1.0");
        headers.set("ce-id", id32());
        headers.set("ce-source", "server-e2e-test");
        headers.set("ce-type", "com.example.OrderCreated");
        headers.set("ce-subject", TOPIC);

        ResponseEntity<String> response = this.rest.postForEntity(
                "/api/v1/publish/" + SOURCE,
                new HttpEntity<>("{\"orderId\":\"o-2\"}", headers),
                String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void disallowedTopicRejected() {
        awaitSourceReady();

        ResponseEntity<ErrorResponse> response = postStructured(
                "server-e2e-others", "{\"orderId\":\"o-3\"}", ErrorResponse.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("topic_not_allowed_error", response.getBody().error());
    }

    /**
     * source 由 Coordinator 按 5s 节拍 reconcile 后才挂到 router；轮询探针发布直到
     * 拿到 200（全链路打通），不 sleep。探针消息本身也会落 stream，不影响断言。
     */
    private void awaitSourceReady() {
        if (this.sourceReady) {
            return;
        }
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    ResponseEntity<String> probe = postStructured(TOPIC, "{\"probe\":true}", String.class);
                    assertEquals(HttpStatus.OK, probe.getStatusCode());
                });
        this.sourceReady = true;
    }

    private <T> ResponseEntity<T> postStructured(String subject, String dataJson, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("application/cloudevents+json"));
        String body = """
                {
                  "specversion": "1.0",
                  "id": "%s",
                  "source": "server-e2e-test",
                  "type": "com.example.OrderCreated",
                  "subject": "%s",
                  "datacontenttype": "application/json",
                  "data": %s
                }
                """.formatted(id32(), subject, dataJson);
        return this.rest.postForEntity("/api/v1/publish/" + SOURCE, new HttpEntity<>(body, headers), responseType);
    }

    private static String id32() {
        return UUID.randomUUID().toString().replace("-", "");
    }

}
