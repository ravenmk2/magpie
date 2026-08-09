package ravenworks.magpie.engine.impl.sink.http;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;
import ravenworks.magpie.engine.api.stream.MessageRecord;

import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class HttpSinkHandlerTest {

    @Test
    void fixedBackoffAlwaysReturnsDelay() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("fixed", 1_000, 30_000, 1));
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("fixed", 1_000, 30_000, 5));
    }

    @Test
    void unknownBackoffFallsBackToFixed() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("jitter", 1_000, 30_000, 3));
    }

    @Test
    void exponentialDoublesPerAttempt() {
        assertEquals(1_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 1));
        assertEquals(2_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 2));
        assertEquals(4_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 3));
    }

    @Test
    void exponentialBackoffIsCaseInsensitive() {
        assertEquals(2_000, HttpSinkHandler.computeBackoffDelay("EXPONENTIAL", 1_000, 30_000, 2));
    }

    @Test
    void exponentialIsCappedAtMaxDelay() {
        assertEquals(30_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, 10));
    }

    @Test
    void exponentialDoesNotWrapOrOverflowAtHighAttempts() {
        // long 移位按 64 取模，attempt >= 64 时 1L << (attempt - 1) 会回绕，必须仍封顶
        for (int attempt : new int[]{63, 64, 65, 100, Integer.MAX_VALUE}) {
            assertEquals(30_000, HttpSinkHandler.computeBackoffDelay("exponential", 1_000, 30_000, attempt),
                    "attempt=" + attempt);
        }
    }

    @Test
    void buildCloudEventMapsAllFields() {
        var record = new ConsumerRecord()
                .setOffset(42L)
                .setMessage(new MessageRecord()
                        .setId("id-1")
                        .setType("t.order.created")
                        .setTopic("orders")
                        .setEventTime(LocalDateTime.of(2026, 8, 4, 12, 30, 45))
                        .setTenantId("tenant-1")
                        .setBusinessKey("bk-1")
                        .setHeaders(Map.of("h1", "v1"))
                        .setPayload("{\"a\":1}".getBytes(StandardCharsets.UTF_8)));

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertEquals("id-1", event.getId());
        assertEquals("t.order.created", event.getType());
        assertEquals("orders", event.getSubject());
        assertEquals("application/json", event.getDataContentType());
        assertNotNull(event.getTime());
        assertNotNull(event.getData());
        assertEquals("tenant-1", event.getExtension("xtenantid"));
        assertEquals("bk-1", event.getExtension("xbusinesskey"));
        assertEquals("42", event.getExtension("xoffset"));
        assertNotNull(event.getExtension("xheaders"));
    }

    @Test
    void buildCloudEventOmitsAbsentFields() {
        var record = new ConsumerRecord()
                .setOffset(7L)
                .setMessage(new MessageRecord()
                        .setId("id-2")
                        .setType("t.ping")
                        .setTopic("orders"));

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertEquals("id-2", event.getId());
        assertNull(event.getTime());
        assertNull(event.getData());
        assertNull(event.getExtension("xtenantid"));
        assertNull(event.getExtension("xbusinesskey"));
        assertNull(event.getExtension("xheaders"));
        assertEquals("7", event.getExtension("xoffset"));
    }

    @Test
    void buildCloudEventOmitsBlankStringsAndEmptyHeaders() {
        var record = new ConsumerRecord()
                .setOffset(1L)
                .setMessage(new MessageRecord()
                        .setId("id-3")
                        .setType("t.ping")
                        .setTopic("orders")
                        .setTenantId(" ")
                        .setBusinessKey("")
                        .setHeaders(Map.of()));

        var event = HttpSinkHandler.buildCloudEvent(record);
        assertNull(event.getExtension("xtenantid"));
        assertNull(event.getExtension("xbusinesskey"));
        assertNull(event.getExtension("xheaders"));
    }

    private static HttpSinkHandler newHandler(String url, int maxAttempts) {
        var config = HttpSinkHandlerConfig.of(Map.of("url", url));
        config.setDelayMs(0);
        config.setMaxAttempts(maxAttempts);
        return new HttpSinkHandler("t", HttpClient.newHttpClient(),
                new CircuitBreaker("t", 100, 1, 1_000), config);
    }

    @Test
    void unserializableRecordFailsOnlyItselfWithoutCircuitBreaker() {
        var handler = newHandler("http://127.0.0.1:1/x", 1);
        try {
            // id 缺失, CloudEvent 构建必失败: 不经 HTTP(attempts=0), 仅本条 FAILURE
            var record = new ConsumerRecord()
                    .setOffset(0)
                    .setMessage(new MessageRecord()
                            .setType("t.test")
                            .setTopic("topic"));
            var result = handler.handle(record).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(0, result.getAttempts());
            assertNotNull(result.getError());
            assertEquals(record, result.getRecord());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void invalidUrlIsRetriedLikeSystemicFailure() {
        var handler = newHandler("://invalid-url", 2);
        try {
            var record = new ConsumerRecord()
                    .setOffset(1)
                    .setMessage(new MessageRecord()
                            .setId("id-x")
                            .setType("t.test")
                            .setTopic("topic"));
            // IllegalArgumentException 与 IO 错误同属系统性故障: 退避重试直到 maxAttempts
            var result = handler.handle(record).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(2, result.getAttempts());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void batchIsolatesPoisonedAndFailingRecords() {
        var handler = newHandler("http://127.0.0.1:1/x", 1);
        try {
            var poisoned = new ConsumerRecord()
                    .setOffset(0)
                    .setMessage(new MessageRecord()
                            .setType("t.test")
                            .setTopic("topic")); // id 缺失, 序列化失败
            var unreachable = new ConsumerRecord()
                    .setOffset(1)
                    .setMessage(new MessageRecord()
                            .setId("id-ok")
                            .setType("t.test")
                            .setTopic("topic")); // 端点连不通, 系统性失败
            // join 不抛异常: 各条的失败被隔离为各自的 FAILURE, 不连坐整批
            var results = handler.handle(List.of(poisoned, unreachable)).join();
            assertEquals(2, results.size());
            assertEquals(SinkStatus.FAILURE, results.get(0).getStatus());
            assertEquals(0, results.get(0).getAttempts());
            assertEquals(SinkStatus.FAILURE, results.get(1).getStatus());
            assertEquals(1, results.get(1).getAttempts());
        } finally {
            handler.shutdown().join();
        }
    }

    /** 脚本化的一次 send：固定返回状态码、延迟返回，或直接抛异常 */
    private record Step(Integer status, Throwable error, long delayMs) {
        static Step respond(int status) {
            return new Step(status, null, 0);
        }

        static Step respondAfter(int status, long delayMs) {
            return new Step(status, null, delayMs);
        }

        static Step fail(Throwable error) {
            return new Step(null, error, 0);
        }
    }

    static class StubHttpClient extends HttpClient {

        final AtomicInteger sendCount = new AtomicInteger();
        private final Queue<Step> script = new ConcurrentLinkedQueue<>();
        private volatile Step repeat; // 脚本耗尽后重复使用（无限重试场景）

        StubHttpClient then(Step... steps) {
            this.script.addAll(List.of(steps));
            return this;
        }

        StubHttpClient thenRepeat(Step step) {
            this.repeat = step;
            return this;
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
                throws IOException, InterruptedException {
            this.sendCount.incrementAndGet();
            var step = this.script.poll();
            if (step == null) {
                step = this.repeat;
            }
            if (step == null) {
                throw new AssertionError("no scripted response left");
            }
            if (step.delayMs() > 0) {
                Thread.sleep(step.delayMs());
            }
            if (step.error() instanceof IOException io) {
                throw io;
            }
            if (step.error() instanceof InterruptedException ie) {
                throw ie;
            }
            if (step.error() != null) {
                throw new AssertionError(step.error());
            }
            @SuppressWarnings("unchecked")
            var response = (HttpResponse<T>) stubResponse(step.status());
            return response;
        }

        private static HttpResponse<String> stubResponse(int status) {
            return new HttpResponse<>() {
                @Override
                public int statusCode() {
                    return status;
                }

                @Override
                public HttpRequest request() {
                    return null;
                }

                @Override
                public Optional<HttpResponse<String>> previousResponse() {
                    return Optional.empty();
                }

                @Override
                public HttpHeaders headers() {
                    return HttpHeaders.of(Map.of(), (a, b) -> true);
                }

                @Override
                public String body() {
                    return "";
                }

                @Override
                public Optional<SSLSession> sslSession() {
                    return Optional.empty();
                }

                @Override
                public URI uri() {
                    return URI.create("http://stub");
                }

                @Override
                public Version version() {
                    return Version.HTTP_1_1;
                }
            };
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request,
                                                                HttpResponse.BodyHandler<T> handler) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request, HttpResponse.BodyHandler<T> handler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public SSLContext sslContext() {
            return null;
        }

        @Override
        public SSLParameters sslParameters() {
            return null;
        }

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

    }

    private static HttpSinkHandler newHandler(HttpClient client, CircuitBreaker breaker,
                                              String url, int maxAttempts, long delayMs) {
        var config = HttpSinkHandlerConfig.of(Map.of("url", url));
        config.setDelayMs(delayMs);
        config.setMaxAttempts(maxAttempts);
        return new HttpSinkHandler("t", client, breaker, config);
    }

    private static ConsumerRecord liveRecord() {
        return new ConsumerRecord()
                .setOffset(1)
                .setMessage(new MessageRecord()
                        .setId("id-live")
                        .setType("t.test")
                        .setTopic("topic")
                        .setPayload("{}".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void okResponseSucceedsAndResetsCircuitBreaker() {
        var client = new StubHttpClient().then(Step.respond(200));
        var breaker = new CircuitBreaker("t", 2, 1, 60_000);
        breaker.recordFailure(); // 距熔断仅差 1 次失败
        var handler = newHandler(client, breaker, "http://stub/ok", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.SUCCESS, result.getStatus());
            assertEquals(1, result.getAttempts());
            assertEquals(1, client.sendCount.get());
            // 2xx 触发 recordSuccess 重置了连续失败计数：再失败一次仍不熔断
            breaker.recordFailure();
            assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void nonRetryable4xxFailsImmediatelyWithoutRetry() {
        // 默认重试码为 500-599,408,429；400 不在其中，立即失败不重试
        var client = new StubHttpClient().then(Step.respond(400));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/bad", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(1, result.getAttempts());
            assertEquals("HTTP 400", result.getError());
            assertEquals(1, client.sendCount.get());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void retryable5xxRetriesUntilMaxAttempts() {
        var client = new StubHttpClient()
                .then(Step.respond(503), Step.respond(503), Step.respond(503));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/flaky", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.FAILURE, result.getStatus());
            assertEquals(3, result.getAttempts());
            assertEquals(3, client.sendCount.get());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void openCircuitBreakerYieldsBackoffWithoutSending() {
        var breaker = new CircuitBreaker("t", 1, 1, 60_000);
        breaker.recordFailure(); // 阈值 1: 立即熔断
        var client = new StubHttpClient().then(Step.respond(200));
        var handler = newHandler(client, breaker, "http://stub/ok", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.BACKOFF, result.getStatus());
            assertEquals(0, result.getAttempts());
            assertEquals(0, client.sendCount.get()); // 熔断时循环顶部直接退出, 未发出请求
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void maxAttemptsZeroRetriesUnlimitedUntilShutdown() {
        // maxAttempts = 0 表示不限次数: handle 永不自行完成, 只能由 shutdown 打断
        var client = new StubHttpClient().thenRepeat(Step.respond(503));
        var handler = newHandler(client, new CircuitBreaker("t", 1_000, 1, 60_000),
                "http://stub/down", 0, 20);
        var future = handler.handle(liveRecord());
        try {
            await().atMost(2, TimeUnit.SECONDS).until(() -> client.sendCount.get() >= 3);
            assertFalse(future.isDone());
        } finally {
            handler.shutdown().join();
        }
        var result = future.join();
        assertEquals(SinkStatus.INTERRUPTED, result.getStatus());
        assertEquals(client.sendCount.get(), result.getAttempts());
    }

    @Test
    void handleAfterShutdownReturnsInterruptedImmediately() {
        var client = new StubHttpClient().then(Step.respond(200));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/ok", 3, 0);
        handler.shutdown().join();

        var result = handler.handle(liveRecord()).join();
        assertEquals(SinkStatus.INTERRUPTED, result.getStatus());
        assertEquals(0, result.getAttempts());
        assertEquals(0, client.sendCount.get());
    }

    @Test
    void shutdownWaitsForPendingRequestsToDrain() {
        var client = new StubHttpClient().then(Step.respondAfter(200, 800));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/slow", 1, 0);
        var future = handler.handle(liveRecord());

        // 等慢响应真正在途后再 shutdown：shutdown 必须等 pendingRequests 排空, 不能提前完成
        await().atMost(2, TimeUnit.SECONDS).until(() -> client.sendCount.get() == 1);
        var shutdownFuture = handler.shutdown();
        assertFalse(shutdownFuture.isDone());
        assertFalse(future.isDone());

        shutdownFuture.join();
        assertEquals(SinkStatus.SUCCESS, future.join().getStatus());
    }

    @Test
    void interruptedSendYieldsInterruptedResult() {
        var client = new StubHttpClient().then(Step.fail(new InterruptedException("stop")));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/ok", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.INTERRUPTED, result.getStatus());
            assertEquals(1, result.getAttempts());
            assertEquals(1, client.sendCount.get());
        } finally {
            handler.shutdown().join();
        }
    }

    @Test
    void ioExceptionIsRetriedLikeSystemicFailure() {
        // IOException 与非法 URL 同属系统性故障: 退避重试, 后续成功则整体 SUCCESS
        var client = new StubHttpClient()
                .then(Step.fail(new IOException("conn reset")), Step.respond(200));
        var handler = newHandler(client, new CircuitBreaker("t", 100, 1, 1_000),
                "http://stub/flaky", 3, 0);
        try {
            var result = handler.handle(liveRecord()).join();
            assertEquals(SinkStatus.SUCCESS, result.getStatus());
            assertEquals(2, result.getAttempts());
            assertEquals(2, client.sendCount.get());
        } finally {
            handler.shutdown().join();
        }
    }

}
