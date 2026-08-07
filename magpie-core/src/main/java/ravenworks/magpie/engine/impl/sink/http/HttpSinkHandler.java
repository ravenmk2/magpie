package ravenworks.magpie.engine.impl.sink.http;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.sink.SinkHandler;
import ravenworks.magpie.engine.api.sink.SinkResult;
import ravenworks.magpie.engine.api.sink.SinkStatus;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class HttpSinkHandler implements SinkHandler {

    private static final URI SOURCE = URI.create("magpie");
    private static final EventFormat JSON_FORMAT =
            EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE);

    private final String name;
    private final String url;
    private final int timeout;
    private final String backoff;
    private final long delayMs;
    private final long maxDelayMs;
    private final int maxAttempts;
    private final Set<Integer> retryStatusCodes;
    private final HttpClient httpClient;
    private final CircuitBreaker circuitBreaker;
    private final ExecutorService executor;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Set<CompletableFuture<SinkResult>> pendingRequests = ConcurrentHashMap.newKeySet();

    public HttpSinkHandler(@NonNull String name,
                           @NonNull HttpClient httpClient,
                           @NonNull CircuitBreaker circuitBreaker,
                           @NonNull HttpSinkHandlerConfig config) {
        this.name = name;
        this.httpClient = httpClient;
        this.circuitBreaker = circuitBreaker;
        this.url = config.getUrl();
        this.timeout = config.getTimeout();
        this.backoff = config.getBackoff();
        this.delayMs = config.getDelayMs();
        this.maxDelayMs = config.getMaxDelayMs();
        this.maxAttempts = config.getMaxAttempts();
        this.retryStatusCodes = config.getRetryStatusCodes();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public CompletableFuture<SinkResult> handle(@NonNull ConsumerRecord record) {
        CompletableFuture<SinkResult> future = new CompletableFuture<>();
        this.pendingRequests.add(future);

        if (this.shutdown.get()) {
            this.pendingRequests.remove(future);
            return CompletableFuture.completedFuture(
                    new SinkResult()
                            .setStatus(SinkStatus.INTERRUPTED)
                            .setRecord(record));
        }

        this.executor.submit(() -> {
            try {
                future.complete(this.doSend(record));
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        future.whenComplete((r, e) -> this.pendingRequests.remove(future));
        return future;
    }

    @Override
    public CompletableFuture<List<SinkResult>> handle(@NonNull List<ConsumerRecord> records) {
        @SuppressWarnings("unchecked")
        CompletableFuture<SinkResult>[] futures = records.stream()
                .map(record -> this.handle(record).exceptionally(e -> {
                    // 兜底隔离: 任何单条消息的异常只影响本条, 不连坐整批
                    log.error("[{}] msgId={} unexpected error, marking as failure",
                            this.name, record.getMessage().getId(), e);
                    return new SinkResult()
                            .setStatus(SinkStatus.FAILURE)
                            .setError("unexpected: " + e)
                            .setRecord(record);
                }))
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(v -> {
                    List<SinkResult> results = new ArrayList<>();
                    for (var future : futures) {
                        results.add(future.join());
                    }
                    return results;
                });
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        this.shutdown.set(true);
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            while (!this.pendingRequests.isEmpty()) {
                LockSupport.parkNanos(10_000_000L);
            }
            this.executor.shutdown();
            future.complete(null);
        });
        return future;
    }

    private SinkResult doSend(ConsumerRecord record) {
        byte[] body;
        try {
            body = JSON_FORMAT.serialize(buildCloudEvent(record));
        } catch (Exception e) {
            // 消息自身非法（如 id 缺失），重试无意义且与端点无关：不计熔断，仅本条失败
            log.error("[{}] msgId={} serialize failed, marking as failure", this.name, record.getMessage().getId(), e);
            return new SinkResult()
                    .setStatus(SinkStatus.FAILURE)
                    .setError("serialize failed: " + e.getMessage())
                    .setRecord(record);
        }
        int attempt = 0;
        while (!this.shutdown.get()) {
            if (this.circuitBreaker.isOpen()) {
                return new SinkResult()
                        .setStatus(SinkStatus.BACKOFF)
                        .setAttempts(attempt)
                        .setRecord(record);
            }
            if (this.maxAttempts > 0 && attempt >= this.maxAttempts) {
                return new SinkResult()
                        .setStatus(SinkStatus.FAILURE)
                        .setAttempts(attempt)
                        .setRecord(record);
            }
            attempt++;

            try {
                var request = HttpRequest.newBuilder()
                        .uri(URI.create(this.url))
                        .timeout(Duration.ofMillis(this.timeout))
                        .header("Content-Type", JsonFormat.CONTENT_TYPE)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build();

                var response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                int statusCode = response.statusCode();

                if (statusCode >= 200 && statusCode < 300) {
                    this.circuitBreaker.recordSuccess();
                    return new SinkResult()
                            .setStatus(SinkStatus.SUCCESS)
                            .setAttempts(attempt)
                            .setRecord(record);
                }

                if (!this.retryStatusCodes.contains(statusCode)) {
                    this.circuitBreaker.recordFailure();
                    log.warn("[{}] msgId={} HTTP {} is not retryable, giving up",
                            this.name, record.getMessage().getId(), statusCode);
                    return new SinkResult()
                            .setStatus(SinkStatus.FAILURE)
                            .setAttempts(attempt)
                            .setError("HTTP " + statusCode)
                            .setRecord(record);
                }

                this.circuitBreaker.recordFailure();
                long backoffDelay = computeBackoffDelay(this.backoff, this.delayMs, this.maxDelayMs, attempt);
                log.warn("[{}] msgId={} HTTP {} (attempt {}), retry in {}ms",
                        this.name, record.getMessage().getId(), statusCode, attempt, backoffDelay);
                backoff(backoffDelay);

            } catch (IOException | RuntimeException e) {
                // RuntimeException（非法 URL、请求构建失败等）与 IO 错误同为系统性故障，同样退避重试
                this.circuitBreaker.recordFailure();
                long backoffDelay = computeBackoffDelay(this.backoff, this.delayMs, this.maxDelayMs, attempt);
                log.warn("[{}] msgId={} send error (attempt {}), retry in {}ms: {}",
                        this.name, record.getMessage().getId(), attempt, backoffDelay, e.toString());
                backoff(backoffDelay);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new SinkResult()
                        .setStatus(SinkStatus.INTERRUPTED)
                        .setAttempts(attempt)
                        .setRecord(record);
            }
        }
        return new SinkResult()
                .setStatus(SinkStatus.INTERRUPTED)
                .setAttempts(attempt)
                .setRecord(record);
    }

    private void backoff(long delayMs) {
        long deadline = System.nanoTime() + delayMs * 1_000_000L;
        while (!this.shutdown.get() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(200_000_000L);
        }
    }

    static CloudEvent buildCloudEvent(ConsumerRecord record) {
        var message = record.getMessage();
        var builder = CloudEventBuilder.v1()
                .withId(message.getId())
                .withSource(SOURCE)
                .withType(message.getType())
                .withSubject(message.getTopic())
                .withDataContentType("application/json");

        if (message.getEventTime() != null) {
            builder.withTime(message.getEventTime().atZone(ZoneId.systemDefault()).toOffsetDateTime());
        }
        if (message.getPayload() != null) {
            builder.withData(message.getPayload());
        }
        if (message.getTenantId() != null && !message.getTenantId().isBlank()) {
            builder.withExtension("xtenantid", message.getTenantId());
        }
        if (message.getBusinessKey() != null && !message.getBusinessKey().isBlank()) {
            builder.withExtension("xbusinesskey", message.getBusinessKey());
        }
        builder.withExtension("xoffset", String.valueOf(record.getOffset()));
        if (message.getHeaders() != null && !message.getHeaders().isEmpty()) {
            builder.withExtension("xheaders", JsonUtils.encode(message.getHeaders()));
        }

        return builder.build();
    }

    static long computeBackoffDelay(String backoff, long delay, long maxDelay, int attempt) {
        if (!"exponential".equalsIgnoreCase(backoff)) {
            return delay;
        }
        if (attempt <= 1 || delay <= 0) {
            return Math.min(delay, maxDelay);
        }
        // long 移位按 64 取模，且 delay * multiplier 可能溢出，统一提前封顶
        int shift = Math.min(attempt - 1, 62);
        long multiplier = 1L << shift;
        if (delay > Long.MAX_VALUE / multiplier) {
            return maxDelay;
        }
        return Math.min(delay * multiplier, maxDelay);
    }

}
