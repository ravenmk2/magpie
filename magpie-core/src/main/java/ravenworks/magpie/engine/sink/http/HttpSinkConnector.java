package ravenworks.magpie.engine.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.retry.RetryMessageStore;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.common.BestEffortSinkWorker;
import ravenworks.magpie.engine.sink.common.KeyOrderedSinkWorker;
import ravenworks.magpie.engine.sink.common.OrderedSinkWorker;
import ravenworks.magpie.engine.sink.common.SinkWorker;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author Raven
 */
@Slf4j
public class HttpSinkConnector implements SinkConnector {

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final RetryMessageStore retryStore;
    private final String name;
    private final String topic;
    private final HttpSinkProperties config;
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
        this.config = HttpSinkProperties.of(properties);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.config.getTimeout()))
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
                this.name, this.workers.size(), this.config.getUrl(), this.config.resolveOrderingGuarantee());
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
        return switch (this.config.resolveOrderingGuarantee()) {
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
                workerName, consumer, handler, cb, this.config.getBatchSize());
    }

    private KeyOrderedSinkWorker createKeyOrderedWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        var cb = createCircuitBreaker(workerName);
        var handler = new HttpSinkHandler(
                workerName, this.httpClient, cb, createHandlerConfig(this.config.getInplaceAttempts()));
        return new KeyOrderedSinkWorker(
                workerName, consumer, handler, cb, this.retryStore, this.config.getBatchSize());
    }

    private BestEffortSinkWorker createBestEffortWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        var cb = createCircuitBreaker(workerName);
        var handler = new HttpSinkHandler(
                workerName, this.httpClient, cb, createHandlerConfig(this.config.getInplaceAttempts()));
        return new BestEffortSinkWorker(
                workerName, consumer, handler, cb, this.retryStore, this.config.getBatchSize());
    }

    private CircuitBreaker createCircuitBreaker(String workerName) {
        return new CircuitBreaker(workerName,
                this.config.getCircuitBreakerFailureThreshold(),
                this.config.getCircuitBreakerHalfOpenSuccessCount(),
                this.config.getCircuitBreakerResetMs());
    }

    private HttpSinkHandlerConfig createHandlerConfig(int maxAttempts) {
        var cfg = new HttpSinkHandlerConfig();
        cfg.setUrl(this.config.getUrl());
        cfg.setTimeout(this.config.getTimeout());
        cfg.setBackoff(this.config.getBackoff());
        cfg.setDelayMs(this.config.getDelayMs());
        cfg.setMaxDelayMs(this.config.getMaxDelayMs());
        cfg.setMaxAttempts(maxAttempts);
        cfg.setRetryStatusCodes(this.config.getRetryStatusCodes());
        return cfg;
    }

}
