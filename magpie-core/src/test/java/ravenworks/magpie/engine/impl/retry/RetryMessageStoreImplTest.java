package ravenworks.magpie.engine.impl.retry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.domain.JpaTestSupport;
import ravenworks.magpie.domain.repository.MessageLogRepository;
import ravenworks.magpie.domain.repository.RetryMessageRepository;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class RetryMessageStoreImplTest {

    private JpaTestSupport support;
    private MessageLogRepository messageLogRepository;
    private RetryMessageRepository retryMessageRepository;
    private RetryMessageStoreImpl store;

    @BeforeEach
    void setUp() {
        this.support = JpaTestSupport.create("retry-message-store-test");
        this.messageLogRepository = this.support.repository(MessageLogRepository.class);
        this.retryMessageRepository = this.support.repository(RetryMessageRepository.class);
        this.store = new RetryMessageStoreImpl(this.messageLogRepository, this.retryMessageRepository);
    }

    @AfterEach
    void tearDown() {
        this.support.close();
    }

    @Test
    void nullMessageIdGeneratesUuid7() {
        var id = RetryMessageStoreImpl.normalizeMessageId(null);
        assertTrue(id.matches("[0-9a-f]{32}"), "expected 32-char hex, got: " + id);
        assertNotEquals(id, RetryMessageStoreImpl.normalizeMessageId(null));
    }

    @Test
    void blankMessageIdGeneratesUuid7() {
        assertTrue(RetryMessageStoreImpl.normalizeMessageId("  ").matches("[0-9a-f]{32}"));
    }

    @Test
    void conformingMessageIdIsKept() {
        assertEquals("0123456789abcdef0123456789abcdef",
                RetryMessageStoreImpl.normalizeMessageId("0123456789abcdef0123456789abcdef"));
    }

    @Test
    void longMessageIdThrows() {
        var longId = "x".repeat(40);
        var ex = assertThrows(IllegalArgumentException.class,
                () -> RetryMessageStoreImpl.normalizeMessageId(longId));
        // 错误信息含完整 id，便于排障
        assertTrue(ex.getMessage().contains(longId), "unexpected message: " + ex.getMessage());
    }

    @Test
    void nullToEmptyCoalescesNull() {
        assertEquals("", RetryMessageStoreImpl.nullToEmpty(null));
        assertEquals("v", RetryMessageStoreImpl.nullToEmpty("v"));
    }

    @Test
    void savePersistsNullPayloadAsEmptyAndNullHeadersAsEmptyMap() {
        this.store.save("c1", record(1L, new MessageRecord()
                .setPayload(null)
                .setHeaders(null)));

        var log = this.messageLogRepository.findAll().stream().findFirst().orElseThrow();
        // null payload 落库为空串、null headers 落库为空 Map——重试落库不允许被空值击穿
        assertEquals("", log.getPayload());
        assertNotNull(log.getHeaders());
        assertTrue(log.getHeaders().isEmpty());
    }

    @Test
    void saveDefaultsNullEventTimeToNow() {
        var before = LocalDateTime.now().minusSeconds(1);
        this.store.save("c1", record(1L, new MessageRecord().setEventTime(null)));
        var after = LocalDateTime.now().plusSeconds(1);

        var log = this.messageLogRepository.findAll().stream().findFirst().orElseThrow();
        assertNotNull(log.getEventTime());
        assertTrue(!log.getEventTime().isBefore(before) && !log.getEventTime().isAfter(after),
                "eventTime should be near now, got: " + log.getEventTime());
    }

    @Test
    void saveStoresPayloadBase64EncodedInMessageLog() {
        byte[] payload = "hello magpie".getBytes();
        this.store.save("c1", record(1L, new MessageRecord().setPayload(payload)));

        var log = this.messageLogRepository.findAll().stream().findFirst().orElseThrow();
        assertEquals(Base64.getEncoder().encodeToString(payload), log.getPayload());
        // 通过 list 做解码往返，还原出原始字节
        var records = this.store.list("c1", 10);
        assertEquals(1, records.size());
        assertArrayEquals(payload, records.getFirst().getPayload());
    }

    @Test
    void saveCreatesImmediatelyRetryableRowWithZeroAttempts() throws InterruptedException {
        this.store.save("c1", record(42L, new MessageRecord()));
        // H2 对 TIMESTAMP 四舍五入到微秒且 now() 精度可能打平，留几毫秒余量保证 retry_at < now 稳定成立
        Thread.sleep(20);

        var records = this.store.listRetryable("c1", 10);
        assertEquals(1, records.size(), "新落库条目应立即到期");
        var record = records.getFirst();
        assertEquals(0, record.getAttempts());
        assertEquals(42L, record.getOffset());
        assertNotNull(record.getRetryAt());
        assertFalse(record.getRetryAt().isAfter(LocalDateTime.now().plusSeconds(1)));
    }

    @Test
    void failedIncrementsAttemptsAndPersistsRetryAt() {
        this.store.save("c1", record(1L, new MessageRecord()));
        var retryId = this.retryMessageRepository.findAll().getFirst().getId();
        var retryAt = LocalDateTime.now().plusMinutes(5);

        this.store.failed(retryId, retryAt);

        var entity = this.retryMessageRepository.findById(retryId).orElseThrow();
        assertEquals(1, entity.getAttempts());
        assertTrue(Duration.between(retryAt, entity.getRetryAt()).abs().compareTo(Duration.ofSeconds(1)) < 0,
                "retryAt should be persisted, got: " + entity.getRetryAt());

        // 再次 failed 继续累加
        this.store.failed(retryId, retryAt);
        assertEquals(2, this.retryMessageRepository.findById(retryId).orElseThrow().getAttempts());
    }

    @Test
    void failedOnMissingIdIsNoOpAndDoesNotThrow() {
        assertDoesNotThrow(() -> this.store.failed("missing-retry-id", LocalDateTime.now()));
        assertEquals(0, this.retryMessageRepository.count());
    }

    @Test
    void succeededDeletesRetryRow() {
        this.store.save("c1", record(1L, new MessageRecord()));
        var retryId = this.retryMessageRepository.findAll().getFirst().getId();

        this.store.succeeded(retryId);

        assertTrue(this.retryMessageRepository.findById(retryId).isEmpty());
        assertTrue(this.store.list("c1", 10).isEmpty());
    }

    @Test
    void listSkipsRetryRowWhoseMessageLogIsMissing() {
        this.store.save("c1", record(1L, new MessageRecord().setPayload("a".getBytes())));
        this.store.save("c1", record(2L, new MessageRecord().setPayload("b".getBytes())));
        // 人为制造毒行：删掉 offset=1 对应的 message_log，重试行成为孤儿
        var poisonedRetry = this.retryMessageRepository.findAll().stream()
                .filter(e -> e.getOffset() == 1L).findFirst().orElseThrow();
        this.messageLogRepository.deleteById(poisonedRetry.getLogId());

        var records = this.store.list("c1", 10);

        assertEquals(1, records.size(), "毒行应被跳过，其余行仍正常返回");
        assertEquals(2L, records.getFirst().getOffset());
        assertArrayEquals("b".getBytes(), records.getFirst().getPayload());
    }

    @Test
    void listRoundTripsHeadersAndEventTime() {
        var eventTime = LocalDateTime.now().minusHours(1).withNano(0);
        this.store.save("c1", record(7L, new MessageRecord()
                .setId("0123456789abcdef0123456789abcdef")
                .setType("order-created")
                .setTopic("orders")
                .setTenantId("t1")
                .setBusinessKey("bk-1")
                .setEventTime(eventTime)
                .setHeaders(Map.of("k1", "v1", "k2", "v2"))
                .setPayload("payload".getBytes())));

        var records = this.store.list("c1", 10);
        assertEquals(1, records.size());
        RetryRecord record = records.getFirst();
        assertEquals("0123456789abcdef0123456789abcdef", record.getMessageId());
        assertEquals("order-created", record.getType());
        assertEquals("orders", record.getTopic());
        assertEquals("t1", record.getTenantId());
        assertEquals("bk-1", record.getBusinessKey());
        assertEquals(Map.of("k1", "v1", "k2", "v2"), record.getHeaders());
        assertEquals(eventTime, record.getEventTime());
        assertArrayEquals("payload".getBytes(), record.getPayload());
    }

    @Test
    void listReturnsEmptyWhenNoRows() {
        assertTrue(this.store.list("c1", 10).isEmpty());
        assertTrue(this.store.listRetryable("c1", 10).isEmpty());
    }

    @Test
    void listHonorsCountLimit() {
        this.store.save("c1", record(1L, new MessageRecord()));
        this.store.save("c1", record(2L, new MessageRecord()));
        this.store.save("c1", record(3L, new MessageRecord()));

        var records = this.store.list("c1", 2);

        assertEquals(2, records.size());
        // 按 offset 升序取前两条
        assertEquals(1L, records.get(0).getOffset());
        assertEquals(2L, records.get(1).getOffset());
    }

    @Test
    void listRetryableExcludesRowsWithRetryAtInFuture() throws InterruptedException {
        this.store.save("c1", record(1L, new MessageRecord()));
        this.store.save("c1", record(2L, new MessageRecord()));
        // offset=1 的行推退到未来，只有 offset=2 仍到期
        var futureRetry = this.retryMessageRepository.findAll().stream()
                .filter(e -> e.getOffset() == 1L).findFirst().orElseThrow();
        this.store.failed(futureRetry.getId(), LocalDateTime.now().plusHours(1));
        // 同上：为 H2 的微秒舍入留余量，保证到期行稳定命中 retry_at < now
        Thread.sleep(20);

        var records = this.store.listRetryable("c1", 10);

        assertEquals(1, records.size());
        assertEquals(2L, records.getFirst().getOffset());
        // list（不带到期过滤）仍能看到全部
        assertEquals(2, this.store.list("c1", 10).size());
    }

    private static ConsumerRecord record(long offset, MessageRecord message) {
        return new ConsumerRecord().setOffset(offset).setMessage(message);
    }

}
