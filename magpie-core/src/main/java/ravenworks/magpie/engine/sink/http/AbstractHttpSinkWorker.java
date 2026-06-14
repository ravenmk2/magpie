package ravenworks.magpie.engine.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.json.JsonUtils;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.http.HttpSinkConnector.HttpConfig;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


@Slf4j
abstract class AbstractHttpSinkWorker {

    static final int BATCH_SIZE = 100;
    static final Object POLL_SIGNAL = new Object();

    static final Set<String> RESERVED_CE_KEYS = Set.of(
            "specversion", "id", "source", "type", "time",
            "subject", "datacontenttype", "data", "headers"
    );

    final String name;
    final int partition;
    final StreamConsumer consumer;
    final HttpConfig config;
    final EventLoop eventLoop;
    final AtomicLong received = new AtomicLong();
    final HttpClient httpClient;
    final CircuitBreaker circuitBreaker;

    volatile Thread eventLoopThread;
    volatile boolean stopped;

    AbstractHttpSinkWorker(@NonNull String name,
                           int partition,
                           @NonNull StreamConsumer consumer,
                           @NonNull HttpConfig config) {
        this.name = name;
        this.partition = partition;
        this.consumer = consumer;
        this.config = config;
        this.eventLoop = new EventLoop("snk-" + name, 5_000, this::dispatch);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.timeout))
                .build();
        this.circuitBreaker = new CircuitBreaker(name,
                config.circuitBreakerFailureThreshold,
                config.circuitBreakerHalfOpenSuccessCount,
                config.circuitBreakerResetMs);
    }

    void start() {
        this.consumer.start();
        this.eventLoop.start();
    }

    java.util.concurrent.CompletableFuture<Void> shutdown() {
        this.stopped = true;
        Thread t = this.eventLoopThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof EventLoop.Started) {
            this.eventLoopThread = Thread.currentThread();
            this.eventLoop.enqueue(POLL_SIGNAL);
            return;
        }
        if (event instanceof EventLoop.PreShutdown) {
            onPreShutdown();
            return;
        }
        if (this.stopped) {
            return;
        }
        if (event instanceof EventLoop.Idle || event == POLL_SIGNAL) {
            pollAndProcess();
        }
    }

    protected void onPreShutdown() {
        try {
            this.consumer.stop();
        } catch (Exception e) {
            log.warn("[{}] partition={} error stopping consumer", this.name, this.partition, e);
        }
    }

    abstract void pollAndProcess();

    protected DeliverResult deliver(ConsumerRecord record, int attempt) {
        if (this.circuitBreaker.isOpen()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            return DeliverResult.FAILURE;
        }
        String json = buildCloudEventJson(record);
        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(this.config.url))
                    .timeout(Duration.ofMillis(this.config.timeout))
                    .header("Content-Type", "application/cloudevents+json")
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();

            var response = this.httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                this.circuitBreaker.recordSuccess();
                return DeliverResult.SUCCESS;
            }

            long backoffDelay = computeBackoffDelay(this.config.backoff, this.config.delay,
                    this.config.maxDelay, attempt);
            log.warn("[{}] partition={} offset={} HTTP {} (attempt {}), retry in {}ms",
                    this.name, this.partition, record.getOffset(),
                    response.statusCode(), attempt, backoffDelay);
            this.circuitBreaker.recordFailure();
            LockSupport.parkNanos(backoffDelay * 1_000_000L);
            return DeliverResult.FAILURE;

        } catch (IOException e) {
            long backoffDelay = computeBackoffDelay(this.config.backoff, this.config.delay,
                    this.config.maxDelay, attempt);
            log.warn("[{}] partition={} offset={} IO error (attempt {}), retry in {}ms: {}",
                    this.name, this.partition, record.getOffset(),
                    attempt, backoffDelay, e.getMessage());
            this.circuitBreaker.recordFailure();
            LockSupport.parkNanos(backoffDelay * 1_000_000L);
            return DeliverResult.FAILURE;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeliverResult.INTERRUPTED;
        }
    }

    protected boolean inplaceRetry(ConsumerRecord record, int maxAttempts) {
        int attempt = 0;
        while (!this.stopped) {
            if (maxAttempts > 0 && attempt >= maxAttempts) {
                return false;
            }
            attempt++;
            DeliverResult result = deliver(record, attempt);
            if (result == DeliverResult.SUCCESS) {
                return true;
            }
            if (result == DeliverResult.INTERRUPTED) {
                return false;
            }
        }
        return false;
    }

    static String buildCloudEventJson(ConsumerRecord record) {
        Map<String, Object> ce = new LinkedHashMap<>();
        ce.put("specversion", "1.0");
        ce.put("id", record.getId());
        ce.put("source", "");
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

    protected enum DeliverResult {
        SUCCESS,
        FAILURE,
        INTERRUPTED
    }

}
