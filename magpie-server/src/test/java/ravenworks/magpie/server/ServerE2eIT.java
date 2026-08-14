package ravenworks.magpie.server;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.NoOffsetException;
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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitUtils;
import ravenworks.magpie.server.dto.ApiResponse;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

// 新版 org.testcontainers.rabbitmq.RabbitMQContainer（2.x）移除了 withPluginsEnabled，
// 启用 stream 插件仍需使用兼容层中的旧类。


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
 * Coordinator reconcile 节拍 10s（Coordinator.DEFAULT_RESYNC_INTERVAL_MS），
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

    /**
     * 生命周期用例专用：禁启 source 观察退订，与主播种数据隔离
     */
    private static final String TOGGLE_TOPIC = "server-e2e-toggle";
    private static final String TOGGLE_SOURCE = "server-e2e-toggle-http";

    /**
     * 发送失败用例专用：删除 topic 行让路由失败，与主播种数据隔离
     */
    private static final String BROKEN_TOPIC = "server-e2e-broken";
    private static final String BROKEN_SOURCE = "server-e2e-broken-http";

    private static final MySQLContainer MYSQL =
            new MySQLContainer(DockerImageName.parse("mysql:8.4"));

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
        this.awaitSourceReady();
        long offsetBefore = this.committedOffset();

        ResponseEntity<String> response = this.postStructured(TOPIC, "{\"orderId\":\"o-1\"}", String.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        // 统一响应信封：成功 success=true、error 为 null、data 为对象
        assertTrue(response.getBody().contains("\"success\":true"),
                "expected success envelope, got: " + response.getBody());

        // print sink 消费这条消息后由 OffsetTrackerImpl 提交 offset（值为 last offset + 1），
        // 断言发布后水位严格增大（排除探针流量提交的干扰），超时覆盖 sink 消费与提交延迟
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertTrue(this.committedOffset() > offsetBefore,
                        "expected committed offset > " + offsetBefore + " for target " + TARGET
                                + ", got " + this.committedOffset()));
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
        this.awaitSourceReady();

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
        this.awaitSourceReady();

        ResponseEntity<ApiResponse> response = this.postStructured(
                "server-e2e-others", "{\"orderId\":\"o-3\"}", ApiResponse.class);
        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("topic_not_allowed_error", response.getBody().error().code());
    }

    /**
     * 入口校验（HttpSourceConnector.validateFieldLengths）：字段超长绝不截断、直接拒，
     * InvalidMessageException → 400 invalid_message_error。
     */
    @Test
    void overlongFieldsRejected() {
        this.awaitSourceReady();

        // id 上限 32（magpie_message_log.message_id CHAR(32)）
        ResponseEntity<ApiResponse> badId = this.postStructured(SOURCE, TOPIC, "a".repeat(33), null,
                "{\"orderId\":\"o-bad-id\"}", ApiResponse.class);
        assertEquals(HttpStatus.BAD_REQUEST, badId.getStatusCode());
        assertNotNull(badId.getBody());
        assertEquals("invalid_message_error", badId.getBody().error().code());
        assertTrue(badId.getBody().error().message().contains("'id'"),
                "expected field name in message, got: " + badId.getBody().error().message());

        // businessKey 上限 256（magpie_message_log.business_key VARCHAR(256)）
        ResponseEntity<ApiResponse> badKey = this.postStructured(SOURCE, TOPIC, id32(), "b".repeat(257),
                "{\"orderId\":\"o-bad-key\"}", ApiResponse.class);
        assertEquals(HttpStatus.BAD_REQUEST, badKey.getStatusCode());
        assertNotNull(badKey.getBody());
        assertEquals("invalid_message_error", badKey.getBody().error().code());
        assertTrue(badKey.getBody().error().message().contains("'xbusinesskey'"),
                "expected field name in message, got: " + badKey.getBody().error().message());
    }

    /**
     * source 注销生命周期：未注册 → 503 no_subscriber_error；播种挂上后 200；
     * is_enabled=0 经 reconcile 退役后再次 503（退订真实生效，而不是仍 200 静默丢消息）。
     */
    @Test
    void sourceDeregistrationReturns503() {
        // 从未播种的 source：router 无订阅者
        ResponseEntity<ApiResponse> unknown = this.postStructured("server-e2e-ghost", TOPIC, id32(), null,
                "{\"orderId\":\"o-4\"}", ApiResponse.class);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, unknown.getStatusCode());
        assertNotNull(unknown.getBody());
        assertEquals("no_subscriber_error", unknown.getBody().error().code());

        // 播种专用 source/topic，等 reconcile 挂上 router 后 200
        this.seedTopicAndSource(TOGGLE_TOPIC, TOGGLE_SOURCE);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    ResponseEntity<String> ok = this.postStructured(TOGGLE_SOURCE, TOGGLE_TOPIC, id32(), null,
                            "{\"orderId\":\"o-5\"}", String.class);
                    assertEquals(HttpStatus.OK, ok.getStatusCode());
                });

        // 禁用后经 reconcile 退订，再发布应为 503 而非 200 静默丢失
        this.jdbc.update("UPDATE magpie_source SET is_enabled = 0 WHERE name = ?", TOGGLE_SOURCE);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    ResponseEntity<ApiResponse> gone = this.postStructured(TOGGLE_SOURCE, TOGGLE_TOPIC, id32(), null,
                            "{\"orderId\":\"o-6\"}", ApiResponse.class);
                    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, gone.getStatusCode());
                    assertNotNull(gone.getBody());
                    assertEquals("no_subscriber_error", gone.getBody().error().code());
                });
    }

    /**
     * stream 发送失败 → 502 publish_failed_error，且消息确实没落 stream。
     *
     * <p>失败注入：删除 magpie_topic 行。RoutingStreamProducer.send 每次经 StreamRegistry
     * 实时查库，行没了即抛 IllegalArgumentException("Unknown topic")，在触碰 broker 前失败；
     * 该 topic 从未成功发布过，producer 缓存为空，失败不受 reconcile 节拍影响（reconcile 只按
     * DB 行建 stream，不会恢复被删的行），在等待窗口内稳定。
     */
    @Test
    void streamSendFailureReturns502() {
        this.seedTopicAndSource(BROKEN_TOPIC, BROKEN_SOURCE);
        String streamName = RabbitUtils.streamQueueName(BROKEN_TOPIC, 0);

        String uri = "rabbitmq-stream://"
                + RABBITMQ.getAdminUsername() + ":" + RABBITMQ.getAdminPassword()
                + "@" + RABBITMQ.getHost() + ":" + RABBITMQ.getMappedPort(STREAM_PORT) + "/%2f";
        try (Environment environment = Environment.builder().uri(uri).build()) {
            // 等 Coordinator 把 broker 侧 stream 建出来，证明后续「没落 stream」断言
            // 针对的是真实存在的 stream，而非「stream 本就不存在」
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertTrue(environment.streamExists(streamName),
                            "expected stream " + streamName + " to be created by Coordinator"));
            long committedBefore = committedOffsetOrEmpty(environment, streamName);

            this.jdbc.update("DELETE FROM magpie_topic WHERE name = ?", BROKEN_TOPIC);

            // source 挂上 router 前是 503，挂上后路由失败稳定为 502；轮询同时覆盖两个收敛
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        ResponseEntity<ApiResponse> failed = this.postStructured(BROKEN_SOURCE, BROKEN_TOPIC,
                                id32(), null, "{\"orderId\":\"o-7\"}", ApiResponse.class);
                        assertEquals(HttpStatus.BAD_GATEWAY, failed.getStatusCode());
                        assertNotNull(failed.getBody());
                        assertEquals("publish_failed_error", failed.getBody().error().code());
                    });

            // 发送在路由阶段即失败，broker 侧 committed offset 不得有变化
            assertEquals(committedBefore, committedOffsetOrEmpty(environment, streamName),
                    "no message should have landed in stream " + streamName);
        }
    }

    /**
     * stream 的 committed offset；空 stream 时 queryStreamStats 抛 NoOffsetException，归一为 -1
     */
    private static long committedOffsetOrEmpty(Environment environment, String streamName) {
        try {
            return environment.queryStreamStats(streamName).committedOffset();
        } catch (NoOffsetException e) {
            return -1;
        }
    }

    /**
     * source 由 Coordinator 按 10s 节拍 reconcile 后才挂到 router；轮询探针发布直到
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
                    ResponseEntity<String> probe = this.postStructured(TOPIC, "{\"probe\":true}", String.class);
                    assertEquals(HttpStatus.OK, probe.getStatusCode());
                });
        this.sourceReady = true;
    }

    private <T> ResponseEntity<T> postStructured(String subject, String dataJson, Class<T> responseType) {
        return this.postStructured(SOURCE, subject, id32(), null, dataJson, responseType);
    }

    private <T> ResponseEntity<T> postStructured(String source, String subject, String id,
                                                 String businessKey, String dataJson, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("application/cloudevents+json"));
        String businessKeyAttribute = businessKey != null
                ? ",\n  \"xbusinesskey\": \"" + businessKey + "\""
                : "";
        String body = """
                {
                  "specversion": "1.0",
                  "id": "%s",
                  "source": "server-e2e-test",
                  "type": "com.example.OrderCreated",
                  "subject": "%s"%s,
                  "datacontenttype": "application/json",
                  "data": %s
                }
                """.formatted(id, subject, businessKeyAttribute, dataJson);
        return this.rest.postForEntity("/api/v1/publish/" + source, new HttpEntity<>(body, headers), responseType);
    }

    /**
     * 播种一对专用 http source / topic（与 @BeforeAll 的主播种数据隔离），供生命周期类用例使用
     */
    private void seedTopicAndSource(String topic, String source) {
        this.jdbc.update("INSERT INTO magpie_topic (id, name, partitions, properties) VALUES (?, ?, ?, ?)",
                id32(), topic, 1, "{}");
        this.jdbc.update("INSERT INTO magpie_source (id, type, name, is_enabled, properties) VALUES (?, ?, ?, ?, ?)",
                id32(), "http", source, 1, "{\"allowedTopics\":[\"" + topic + "\"]}");
    }

    private static String id32() {
        return UUID.randomUUID().toString().replace("-", "");
    }

}
