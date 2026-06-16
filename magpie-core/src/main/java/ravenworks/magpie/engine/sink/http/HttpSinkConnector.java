package ravenworks.magpie.engine.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.sink.OrderingGuarantee;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.common.BestEffortSinkWorker;
import ravenworks.magpie.engine.sink.common.OrderedSinkWorker;
import ravenworks.magpie.engine.sink.common.SinkWorker;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;


/**
 * @author Raven
 */
@Slf4j
public class HttpSinkConnector implements SinkConnector {

    static final String PROP_URL = "url";
    static final String PROP_TIMEOUT = "timeout";
    static final String PROP_RETRY_BACKOFF = "retry.backoff";
    static final String PROP_RETRY_DELAY = "retry.delay";
    static final String PROP_RETRY_MAX_DELAY = "retry.maxDelay";
    static final String PROP_RETRY_STATUS_CODES = "retry.statusCodes";
    static final String PROP_RETRY_INPLACE_ATTEMPTS = "retry.inplaceAttempts";
    static final String PROP_RETRY_EMPTY_POLL_THRESHOLD = "retry.emptyPollThreshold";

    static final String PROP_CIRCUIT_BREAKER_FAILURE_THRESHOLD = "circuitBreaker.failureThreshold";
    static final String PROP_CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES = "circuitBreaker.halfOpenSuccessCount";
    static final String PROP_CIRCUIT_BREAKER_RESET_MS = "circuitBreaker.resetMs";

    static final String PROP_ORDERING_GUARANTEE = "orderingGuarantee";
    static final String PROP_BATCH_SIZE = "batchSize";

    static final String DEFAULT_TIMEOUT = "10000";
    static final String DEFAULT_BACKOFF = "fixed";
    static final String DEFAULT_DELAY = "1000";
    static final String DEFAULT_MAX_DELAY = "30000";
    static final String DEFAULT_STATUS_CODES = "500-599,408,429";
    static final String DEFAULT_INPLACE_ATTEMPTS = "3";
    static final String DEFAULT_EMPTY_POLL_THRESHOLD = "3";
    static final String DEFAULT_FAILURE_THRESHOLD = "10";
    static final String DEFAULT_HALF_OPEN_SUCCESSES = "6";
    static final String DEFAULT_RESET_MS = "600000";
    static final String DEFAULT_ORDERING_GUARANTEE = "ORDERED";
    static final String DEFAULT_BATCH_SIZE = "100";

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final RetryMessageStore retryStore;
    private final String name;
    private final String topic;
    private final HttpConfig config;
    private final HttpClient httpClient;
    private final List<SinkWorker> workers = new ArrayList<>();

    public HttpSinkConnector(@NonNull StreamProvider provider,
                             @NonNull StreamRegistry streamRegistry,
                             @NonNull RetryMessageStore retryStore,
                             @NonNull String name,
                             @NonNull String topic,
                             Map<String, Object> properties) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
        this.retryStore = retryStore;
        this.name = name;
        this.topic = topic;
        this.config = parseConfig(properties);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.config.timeout))
                .build();
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        StreamDefinition definition = this.streamRegistry.getStream(this.topic);
        if (definition == null) {
            log.error("Stream not found: {}", this.topic);
            return;
        }
        var consumers = this.provider.consumer(definition, this.name);
        for (StreamConsumer consumer : consumers) {
            var worker = createWorker(this.name, consumer);
            this.workers.add(worker);
            worker.start();
        }
        log.info("HTTP sink '{}' started, {} worker(s), url={}, ordering={}",
                this.name, this.workers.size(), this.config.url, this.config.orderingGuarantee);
    }

    @Override
    public java.util.concurrent.CompletableFuture<Void> shutdown() {
        var futures = this.workers.stream()
                .map(SinkWorker::shutdown)
                .toArray(java.util.concurrent.CompletableFuture[]::new);
        return java.util.concurrent.CompletableFuture.allOf(futures)
                .thenRun(() -> log.info("HTTP sink '{}' shutdown", this.name));
    }

    private SinkWorker createWorker(String name, StreamConsumer consumer) {
        return switch (this.config.orderingGuarantee) {
            case ORDERED -> createOrderedWorker(name, consumer);
            case KEY_ORDERED -> createKeyOrderedWorker(name, consumer);
            case BEST_EFFORT -> createBestEffortWorker(name, consumer);
        };
    }

    private OrderedSinkWorker createOrderedWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        var cb = createCircuitBreaker(workerName);
        var handler = new HttpSinkHandler(
                workerName, this.httpClient, cb, createHandlerConfig(-1));
        return new OrderedSinkWorker(
                workerName, consumer, handler, cb, this.config.batchSize);
    }

    private KeyOrderedHttpSinkWorker createKeyOrderedWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        return new KeyOrderedHttpSinkWorker(
                workerName, consumer.partition(), consumer, this.retryStore, this.config);
    }

    private BestEffortSinkWorker createBestEffortWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        var cb = createCircuitBreaker(workerName);
        var handler = new HttpSinkHandler(
                workerName, this.httpClient, cb, createHandlerConfig(this.config.retryInplaceAttempts));
        return new BestEffortSinkWorker(
                workerName, consumer, handler, cb, this.retryStore, this.config.batchSize);
    }

    private CircuitBreaker createCircuitBreaker(String workerName) {
        return new CircuitBreaker(workerName,
                this.config.circuitBreakerFailureThreshold,
                this.config.circuitBreakerHalfOpenSuccessCount,
                this.config.circuitBreakerResetMs);
    }

    private HttpSinkHandlerConfig createHandlerConfig(int maxAttempts) {
        var cfg = new HttpSinkHandlerConfig();
        cfg.setUrl(this.config.url);
        cfg.setTimeout(this.config.timeout);
        cfg.setBackoff(this.config.backoff);
        cfg.setDelayMs(this.config.delay);
        cfg.setMaxDelayMs(this.config.maxDelay);
        cfg.setMaxAttempts(maxAttempts);
        cfg.setRetryStatusCodes(this.config.retryStatusCodes);
        return cfg;
    }

    static HttpConfig parseConfig(Map<String, Object> props) {
        if (props == null) {
            throw new IllegalArgumentException("HttpSinkConnector requires 'url' property");
        }
        String url = getString(props, PROP_URL, null);
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("HttpSinkConnector requires 'url' property");
        }
        int timeout = Integer.parseInt(getString(props, PROP_TIMEOUT, DEFAULT_TIMEOUT));
        String backoff = getString(props, PROP_RETRY_BACKOFF, DEFAULT_BACKOFF);
        long delay = Long.parseLong(getString(props, PROP_RETRY_DELAY, DEFAULT_DELAY));
        long maxDelay = Long.parseLong(getString(props, PROP_RETRY_MAX_DELAY, DEFAULT_MAX_DELAY));
        String statusCodesStr = getString(props, PROP_RETRY_STATUS_CODES, DEFAULT_STATUS_CODES);
        Set<Integer> retryStatusCodes = parseStatusCodes(statusCodesStr);
        int inplaceAttempts = Integer.parseInt(getString(props, PROP_RETRY_INPLACE_ATTEMPTS, DEFAULT_INPLACE_ATTEMPTS));
        int emptyPollThreshold = Integer.parseInt(getString(props, PROP_RETRY_EMPTY_POLL_THRESHOLD, DEFAULT_EMPTY_POLL_THRESHOLD));
        int failureThreshold = Integer.parseInt(getString(props, PROP_CIRCUIT_BREAKER_FAILURE_THRESHOLD, DEFAULT_FAILURE_THRESHOLD));
        int halfOpenSuccesses = Integer.parseInt(getString(props, PROP_CIRCUIT_BREAKER_HALF_OPEN_SUCCESSES, DEFAULT_HALF_OPEN_SUCCESSES));
        long resetMs = Long.parseLong(getString(props, PROP_CIRCUIT_BREAKER_RESET_MS, DEFAULT_RESET_MS));
        String orderingGuaranteeStr = getString(props, PROP_ORDERING_GUARANTEE, DEFAULT_ORDERING_GUARANTEE);
        OrderingGuarantee orderingGuarantee = parseOrderingGuarantee(orderingGuaranteeStr);
        int batchSize = Integer.parseInt(getString(props, PROP_BATCH_SIZE, DEFAULT_BATCH_SIZE));
        return new HttpConfig(url, timeout, backoff, delay, maxDelay, retryStatusCodes,
                failureThreshold, halfOpenSuccesses, resetMs,
                inplaceAttempts, emptyPollThreshold, orderingGuarantee, batchSize);
    }

    private static String getString(Map<String, Object> props, String key, String defaultValue) {
        Object val = props.get(key);
        if (val == null) {
            return defaultValue;
        }
        return val.toString();
    }

    static Set<Integer> parseStatusCodes(String str) {
        Set<Integer> codes = new HashSet<>();
        for (String part : str.split(",")) {
            part = part.trim();
            if (part.contains("-")) {
                String[] range = part.split("-", 2);
                int start = Integer.parseInt(range[0].trim());
                int end = Integer.parseInt(range[1].trim());
                for (int c = start; c <= end; c++) {
                    codes.add(c);
                }
            } else if (!part.isEmpty()) {
                codes.add(Integer.parseInt(part));
            }
        }
        return codes;
    }

    private static OrderingGuarantee parseOrderingGuarantee(String value) {
        if (value == null || value.isBlank()) {
            return OrderingGuarantee.ORDERED;
        }
        for (var v : OrderingGuarantee.values()) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }
        return OrderingGuarantee.ORDERED;
    }

    static class HttpConfig {

        final String url;
        final int timeout;
        final String backoff;
        final long delay;
        final long maxDelay;
        final Set<Integer> retryStatusCodes;
        final int circuitBreakerFailureThreshold;
        final int circuitBreakerHalfOpenSuccessCount;
        final long circuitBreakerResetMs;
        final int retryInplaceAttempts;
        final int emptyPollThreshold;
        final OrderingGuarantee orderingGuarantee;
        final int batchSize;

        HttpConfig(String url, int timeout, String backoff,
                   long delay, long maxDelay, Set<Integer> retryStatusCodes,
                   int circuitBreakerFailureThreshold, int circuitBreakerHalfOpenSuccessCount,
                   long circuitBreakerResetMs,
                   int retryInplaceAttempts, int emptyPollThreshold,
                   OrderingGuarantee orderingGuarantee, int batchSize) {
            this.url = url;
            this.timeout = timeout;
            this.backoff = backoff;
            this.delay = delay;
            this.maxDelay = maxDelay;
            this.retryStatusCodes = retryStatusCodes;
            this.circuitBreakerFailureThreshold = circuitBreakerFailureThreshold;
            this.circuitBreakerHalfOpenSuccessCount = circuitBreakerHalfOpenSuccessCount;
            this.circuitBreakerResetMs = circuitBreakerResetMs;
            this.retryInplaceAttempts = retryInplaceAttempts;
            this.emptyPollThreshold = emptyPollThreshold;
            this.orderingGuarantee = orderingGuarantee;
            this.batchSize = batchSize;
        }

    }

}
