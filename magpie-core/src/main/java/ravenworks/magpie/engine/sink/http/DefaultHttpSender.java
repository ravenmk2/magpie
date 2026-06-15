package ravenworks.magpie.engine.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;
import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class DefaultHttpSender implements HttpSender {

    private static final Set<String> RESERVED_CE_KEYS = Set.of(
            "specversion", "id", "source", "type", "time",
            "subject", "datacontenttype", "data", "headers"
    );

    private final String name;
    private final String url;
    private final int timeout;
    private final String backoff;
    private final long delayMs;
    private final long maxDelayMs;
    private final int maxAttempts;
    private final Set<Integer> retryStatusCodes;
    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Set<CompletableFuture<HttpSendResult>> pendingRequests = ConcurrentHashMap.newKeySet();

    public DefaultHttpSender(@NonNull String name,
                             @NonNull HttpSenderConfig config) {
        this.name = name;
        this.url = config.getUrl();
        this.timeout = config.getTimeout();
        this.backoff = config.getBackoff();
        this.delayMs = config.getDelayMs();
        this.maxDelayMs = config.getMaxDelayMs();
        this.maxAttempts = config.getMaxAttempts();
        this.retryStatusCodes = config.getRetryStatusCodes();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.timeout))
                .build();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public CompletableFuture<HttpSendResult> send(@NonNull ConsumerRecord record) {
        CompletableFuture<HttpSendResult> future = new CompletableFuture<>();
        this.pendingRequests.add(future);

        if (this.shutdown.get()) {
            this.pendingRequests.remove(future);
            return CompletableFuture.completedFuture(
                    new HttpSendResult()
                            .setStatus(DeliverStatus.INTERRUPTED)
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
    public CompletableFuture<List<HttpSendResult>> send(@NonNull List<ConsumerRecord> records) {
        @SuppressWarnings("unchecked")
        CompletableFuture<HttpSendResult>[] futures = records.stream()
                .map(this::send)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenApply(v -> {
                    List<HttpSendResult> results = new ArrayList<>();
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
            try {
                this.httpClient.close();
            } catch (Exception e) {
                log.warn("[{}] Error closing HttpClient", this.name, e);
            }
            this.executor.shutdown();
            future.complete(null);
        });
        return future;
    }

    private HttpSendResult doSend(ConsumerRecord record) {
        int attempt = 0;
        while (!this.shutdown.get()) {
            if (this.maxAttempts > 0 && attempt >= this.maxAttempts) {
                return new HttpSendResult()
                        .setStatus(DeliverStatus.FAILURE)
                        .setAttempts(attempt)
                        .setRecord(record);
            }
            attempt++;

            String json = buildCloudEventJson(record);
            try {
                var request = HttpRequest.newBuilder()
                        .uri(URI.create(this.url))
                        .timeout(Duration.ofMillis(this.timeout))
                        .header("Content-Type", "application/cloudevents+json")
                        .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                        .build();

                var response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                int statusCode = response.statusCode();

                if (statusCode >= 200 && statusCode < 300) {
                    return new HttpSendResult()
                            .setStatus(DeliverStatus.SUCCESS)
                            .setAttempts(attempt)
                            .setRecord(record);
                }

                if (!this.retryStatusCodes.contains(statusCode)) {
                    log.warn("[{}] msgId={} HTTP {} is not retryable, giving up",
                            this.name, record.getId(), statusCode);
                    return new HttpSendResult()
                            .setStatus(DeliverStatus.FAILURE)
                            .setAttempts(attempt)
                            .setError("HTTP " + statusCode)
                            .setRecord(record);
                }

                long backoffDelay = computeBackoffDelay(this.backoff, this.delayMs, this.maxDelayMs, attempt);
                log.warn("[{}] msgId={} HTTP {} (attempt {}), retry in {}ms",
                        this.name, record.getId(), statusCode, attempt, backoffDelay);
                backoff(backoffDelay);

            } catch (IOException e) {
                long backoffDelay = computeBackoffDelay(this.backoff, this.delayMs, this.maxDelayMs, attempt);
                log.warn("[{}] msgId={} IO error (attempt {}), retry in {}ms: {}",
                        this.name, record.getId(), attempt, backoffDelay, e.getMessage());
                backoff(backoffDelay);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new HttpSendResult()
                        .setStatus(DeliverStatus.INTERRUPTED)
                        .setAttempts(attempt)
                        .setRecord(record);
            }
        }
        return new HttpSendResult()
                .setStatus(DeliverStatus.INTERRUPTED)
                .setAttempts(attempt)
                .setRecord(record);
    }

    private void backoff(long delayMs) {
        long deadline = System.nanoTime() + delayMs * 1_000_000L;
        while (!this.shutdown.get() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(200_000_000L);
        }
    }

    static String buildCloudEventJson(ConsumerRecord record) {
        Map<String, Object> ce = new LinkedHashMap<>();
        ce.put("specversion", "1.0");
        ce.put("id", record.getId());
        ce.put("source", "magpie");
        ce.put("type", record.getType());
        if (record.getEventTime() != null) {
            ce.put("time", record.getEventTime().toString());
        }
        ce.put("subject", record.getTopic());
        ce.put("datacontenttype", "application/json");

        String payloadStr = record.getPayload() != null
                ? new String(record.getPayload(), StandardCharsets.UTF_8)
                : "";
        ce.put("data", payloadStr);

        Map<String, String> extHeaders = new LinkedHashMap<>();
        if (record.getHeaders() != null) {
            for (var entry : record.getHeaders().entrySet()) {
                if (!RESERVED_CE_KEYS.contains(entry.getKey().toLowerCase())) {
                    extHeaders.put(entry.getKey(), entry.getValue());
                }
            }
        }
        ce.put("headers", extHeaders);

        return JsonUtils.encode(ce);
    }

    static long computeBackoffDelay(String backoff, long delay, long maxDelay, int attempt) {
        if ("exponential".equalsIgnoreCase(backoff)) {
            long computed = delay * (1L << (attempt - 1));
            return Math.min(computed, maxDelay);
        }
        return delay;
    }


}
