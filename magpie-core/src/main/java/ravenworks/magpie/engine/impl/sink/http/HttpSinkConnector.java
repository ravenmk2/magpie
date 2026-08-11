package ravenworks.magpie.engine.impl.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.sink.SinkWorker;
import ravenworks.magpie.engine.impl.sink.deliverer.BestEffortDeliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.Deliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.KeyOrderedDeliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.OrderedDeliverer;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * HTTP sink 连接器：内置调谐循环（reconcile），按 stream 分区集自主维持
 * 每分区一个 {@link SinkWorker}。worker 死亡由本连接器原地重建
 * （新 consumer 从已提交 offset 续传，at-least-once 语义不变），
 * 不上报为连接器死亡；{@link #isAlive()} 只反映调谐循环自身存活，
 * 循环死亡时才由 Coordinator 整体重建本连接器。
 *
 * @author Raven
 */
@Slf4j
public class HttpSinkConnector implements SinkConnector {

    private static final int DEFAULT_RECONCILE_INTERVAL_MS = 10_000;
    private static final long WORKER_SHUTDOWN_TIMEOUT_MS = 10_000;

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final RetryMessageStore retryStore;
    private final String name;
    private final String topic;
    private final HttpSinkProperties config;
    private final HttpClient httpClient;
    private final WorkLoop reconcileLoop;
    private final long workerShutdownTimeoutMs;

    // 以下字段全部 confined 在 reconcileLoop 线程（dispatch 回调内）访问，无需同步
    private final Map<Integer, SinkWorker> workers = new LinkedHashMap<>();
    private boolean streamMissingLogged;

    public HttpSinkConnector(@NonNull StreamProvider provider,
                             @NonNull StreamRegistry streamRegistry,
                             @NonNull RetryMessageStore retryStore,
                             @NonNull String name,
                             @NonNull String topic,
                             Map<String, Object> properties) {
        this(provider, streamRegistry, retryStore, name, topic, properties,
                DEFAULT_RECONCILE_INTERVAL_MS, WORKER_SHUTDOWN_TIMEOUT_MS);
    }

    HttpSinkConnector(@NonNull StreamProvider provider,
                      @NonNull StreamRegistry streamRegistry,
                      @NonNull RetryMessageStore retryStore,
                      @NonNull String name,
                      @NonNull String topic,
                      Map<String, Object> properties,
                      int reconcileIntervalMs,
                      long workerShutdownTimeoutMs) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
        this.retryStore = retryStore;
        this.name = name;
        this.topic = topic;
        this.config = HttpSinkProperties.of(properties);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.config.getTimeout()))
                .build();
        this.workerShutdownTimeoutMs = workerShutdownTimeoutMs;
        this.reconcileLoop = new WorkLoop("snk-" + name, reconcileIntervalMs, this::dispatch);
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
        this.reconcileLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.reconcileLoop.shutdown();
    }

    @Override
    public boolean isAlive() {
        return this.reconcileLoop.isAlive();
    }

    private void dispatch(Object event) {
        if (event instanceof WorkLoopSignal signal) {
            switch (signal) {
                case STARTED, IDLE -> this.reconcile();
                case PRE_SHUTDOWN -> this.stopAllWorkers();
                case TERMINATED -> {
                }
            }
        }
    }

    /**
     * 水平收敛：期望态为 stream 分区集，实际态为运行中的 worker 映射。
     * 先退役（partition 退出期望或 worker 已死亡）并等待关停完成，再补齐缺口，
     * 避免同分区新旧 worker 并存。
     */
    private void reconcile() {
        StreamDefinition definition = this.streamRegistry.getStream(this.topic);
        if (definition == null) {
            if (!this.streamMissingLogged) {
                log.warn("HTTP sink '{}': stream '{}' not found, will retry on next reconcile",
                        this.name, this.topic);
                this.streamMissingLogged = true;
            }
            this.retireWorkers(Set.of());
            return;
        }
        this.streamMissingLogged = false;
        Set<Integer> desired = new HashSet<>();
        for (int i = 0; i < definition.partitions(); i++) {
            desired.add(i);
        }
        this.retireWorkers(desired);
        this.startMissingWorkers(definition, desired);
    }

    private void retireWorkers(Set<Integer> desired) {
        var it = this.workers.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            boolean inDesired = desired.contains(entry.getKey());
            if (inDesired && entry.getValue().isAlive()) {
                continue;
            }
            it.remove();
            if (inDesired) {
                log.info("HTTP sink '{}': worker for partition {} is not alive, restarting",
                        this.name, entry.getKey());
            } else {
                log.info("HTTP sink '{}': partition {} is out of desired state, stopping worker",
                        this.name, entry.getKey());
            }
            this.awaitWorkerShutdown(entry.getValue());
        }
    }

    private void startMissingWorkers(StreamDefinition definition, Set<Integer> desired) {
        for (int partition : desired) {
            if (this.workers.containsKey(partition)) {
                continue;
            }
            try {
                var consumer = this.provider.consumer(definition, partition, this.name);
                var worker = this.createWorker(this.name, consumer);
                worker.start();
                this.workers.put(partition, worker);
                log.info("HTTP sink '{}': worker for partition {} started", this.name, partition);
            } catch (Exception e) {
                log.error("HTTP sink '{}': failed to start worker for partition {},"
                        + " will retry on next reconcile", this.name, partition, e);
            }
        }
    }

    private void stopAllWorkers() {
        for (var worker : this.workers.values()) {
            this.awaitWorkerShutdown(worker);
        }
        this.workers.clear();
    }

    private void awaitWorkerShutdown(SinkWorker worker) {
        try {
            worker.shutdown().get(this.workerShutdownTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("HTTP sink '{}': worker shutdown interrupted", this.name);
        } catch (Exception e) {
            log.warn("HTTP sink '{}': worker did not stop within {} ms",
                    this.name, this.workerShutdownTimeoutMs, e);
        }
    }

    private SinkWorker createWorker(String name, StreamConsumer consumer) {
        var workerName = name + "-" + consumer.partition();
        var cb = this.createCircuitBreaker(workerName);
        int maxAttempts = switch (this.config.resolveDeliveryMode()) {
            case ORDERED -> -1;
            default -> this.config.getInplaceAttempts();
        };
        var handler = new HttpSinkHandler(
                workerName, this.httpClient, cb, this.createHandlerConfig(maxAttempts));
        Deliverer deliverer = switch (this.config.resolveDeliveryMode()) {
            case ORDERED -> new OrderedDeliverer(workerName, handler, cb);
            case KEY_ORDERED -> new KeyOrderedDeliverer(
                    workerName, handler, this.config.getBatchSize(), cb, this.retryStore,
                    Duration.ofMillis(this.config.getPersistRetryDelayMs()));
            case BEST_EFFORT -> new BestEffortDeliverer(
                    workerName, handler, this.config.getBatchSize(), cb, this.retryStore,
                    Duration.ofMillis(this.config.getPersistRetryDelayMs()));
        };
        return new SinkWorker(workerName, consumer, this.config.getBatchSize(),
                this.config.getCommitInterval(), deliverer);
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
