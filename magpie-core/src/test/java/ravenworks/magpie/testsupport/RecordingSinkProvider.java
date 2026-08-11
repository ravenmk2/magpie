package ravenworks.magpie.testsupport;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.DeliveryMode;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.sink.SinkWorker;
import ravenworks.magpie.engine.impl.sink.deliverer.BestEffortDeliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.Deliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.KeyOrderedDeliverer;
import ravenworks.magpie.engine.impl.sink.deliverer.OrderedDeliverer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * 录制型 SinkProvider（IT 用）：type="recording"。装配方式对齐
 * {@code HttpSinkConnector}（streamRegistry 取 StreamDefinition、每分区一个 consumer
 * 与一个 SinkWorker、按 deliveryMode 选 Deliverer、同样的 CircuitBreaker 构造），
 * 只是 handler 换成 {@link RecordingSinkHandler}，properties 键与缺省值也对齐
 * http sink（deliveryMode / batchSize / commit.interval / circuitBreaker.*）。
 *
 * <p>每次 {@link #create} 新建一批 handler 并按 target name 登记，测试据此取回 handler
 * 做断言与故障注入；Connector 重建（如引擎重启）会产生新的、未带失败规则的 handler。
 */
@Slf4j
public class RecordingSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;
    private final RetryMessageStore retryStore;
    private final Map<String, List<RecordingSinkHandler>> handlersByTarget = new ConcurrentHashMap<>();

    public RecordingSinkProvider(@NonNull StreamRegistry streamRegistry,
                                 @NonNull RetryMessageStore retryStore) {
        this.streamRegistry = streamRegistry;
        this.retryStore = retryStore;
    }

    @Override
    public String type() {
        return "recording";
    }

    @Override
    public SinkConnector create(StreamProvider provider, TargetDefinition definition) {
        return new RecordingSinkConnector(provider, definition);
    }

    /**
     * 该 target 历次 create 产生的全部 handler（按创建顺序）；未创建过返回空表。
     */
    public List<RecordingSinkHandler> handlers(String targetName) {
        return List.copyOf(this.handlersByTarget.getOrDefault(targetName, List.of()));
    }

    /**
     * 该 target 最近一次 create 产生的 handler；未创建过返回 null。
     * 测试可用 Awaitility 等它非空（sink 由 Coordinator 异步拉起）。
     */
    public RecordingSinkHandler latestHandler(String targetName) {
        var handlers = this.handlersByTarget.get(targetName);
        return handlers == null || handlers.isEmpty() ? null : handlers.getLast();
    }

    private void register(String targetName, RecordingSinkHandler handler) {
        this.handlersByTarget
                .computeIfAbsent(targetName, k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }


    /**
     * properties 键与缺省值对齐 {@code HttpSinkProperties}（去掉 url/超时/状态码等
     * HTTP 专属项），deliveryMode 解析逻辑一致（缺省/非法回落 ORDERED）。
     */
    @Data
    public static class RecordingSinkProperties {

        @JsonProperty("deliveryMode")
        private String deliveryMode = "ORDERED";

        @JsonProperty("batchSize")
        private int batchSize = 100;

        @JsonProperty("commit.interval")
        private long commitInterval = 30000;

        @JsonProperty("circuitBreaker.failureThreshold")
        private int circuitBreakerFailureThreshold = 10;

        @JsonProperty("circuitBreaker.halfOpenSuccessCount")
        private int circuitBreakerHalfOpenSuccessCount = 6;

        @JsonProperty("circuitBreaker.resetMs")
        private long circuitBreakerResetMs = 600000;

        public static RecordingSinkProperties of(Map<String, Object> props) {
            var config = new RecordingSinkProperties();
            PropertiesUtils.bind(config, props);
            return config;
        }

        public DeliveryMode resolveDeliveryMode() {
            if (this.deliveryMode == null || this.deliveryMode.isBlank()) {
                return DeliveryMode.ORDERED;
            }
            for (var v : DeliveryMode.values()) {
                if (v.name().equalsIgnoreCase(this.deliveryMode)) {
                    return v;
                }
            }
            return DeliveryMode.ORDERED;
        }

    }


    private class RecordingSinkConnector implements SinkConnector {

        private final StreamProvider provider;
        private final String name;
        private final String topic;
        private final RecordingSinkProperties config;
        private final List<SinkWorker> workers = new ArrayList<>();

        RecordingSinkConnector(StreamProvider provider, TargetDefinition definition) {
            this.provider = provider;
            this.name = definition.getName();
            this.topic = definition.getTopic();
            this.config = RecordingSinkProperties.of(definition.getProperties());
        }

        @Override
        public String type() {
            return "recording";
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public void start() {
            StreamDefinition definition = RecordingSinkProvider.this.streamRegistry.getStream(this.topic);
            if (definition == null) {
                log.error("Stream not found: {}", this.topic);
                return;
            }
            var consumers = this.provider.consumer(definition, this.name);
            for (StreamConsumer consumer : consumers) {
                var worker = this.createWorker(consumer);
                this.workers.add(worker);
                worker.start();
            }
            log.info("Recording sink '{}' started, {} worker(s), deliveryMode={}",
                    this.name, this.workers.size(), this.config.resolveDeliveryMode());
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            var futures = this.workers.stream()
                    .map(SinkWorker::shutdown)
                    .toArray(CompletableFuture[]::new);
            return CompletableFuture.allOf(futures)
                    .thenRun(() -> log.info("Recording sink '{}' shutdown", this.name));
        }

        @Override
        public boolean isAlive() {
            return !this.workers.isEmpty()
                    && this.workers.stream().allMatch(SinkWorker::isAlive);
        }

        private SinkWorker createWorker(StreamConsumer consumer) {
            var workerName = this.name + "-" + consumer.partition();
            var cb = new CircuitBreaker(workerName,
                    this.config.getCircuitBreakerFailureThreshold(),
                    this.config.getCircuitBreakerHalfOpenSuccessCount(),
                    this.config.getCircuitBreakerResetMs());
            var handler = new RecordingSinkHandler();
            RecordingSinkProvider.this.register(this.name, handler);
            Deliverer deliverer = switch (this.config.resolveDeliveryMode()) {
                case ORDERED -> new OrderedDeliverer(workerName, handler, cb);
                case KEY_ORDERED -> new KeyOrderedDeliverer(
                        workerName, handler, this.config.getBatchSize(), cb,
                        RecordingSinkProvider.this.retryStore);
                case BEST_EFFORT -> new BestEffortDeliverer(
                        workerName, handler, this.config.getBatchSize(), cb,
                        RecordingSinkProvider.this.retryStore);
            };
            return new SinkWorker(workerName, consumer, this.config.getBatchSize(),
                    this.config.getCommitInterval(), deliverer);
        }

    }

}
