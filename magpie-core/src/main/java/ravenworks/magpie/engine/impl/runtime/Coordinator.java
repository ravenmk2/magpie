package ravenworks.magpie.engine.impl.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.Lifecycle;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.engine.api.election.LeaderElection;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkFactory;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.sink.TargetRegistry;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceFactory;
import ravenworks.magpie.engine.api.source.SourceRegistry;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * 协调器：以 reconcile 循环驱动运行时向期望状态收敛。
 * 触发源（唤醒信号、选举事件、IDLE resync 节拍）只发起收敛，本身不携带状态；
 * 期望状态每轮从 Registry 实时读取，实际状态为运行中的连接器映射
 * （含 isAlive 存活观测：配置未变但已死亡的连接器同样退役重建）。
 * 单个连接器启动失败只影响自身、下轮重试，不连坐其他连接器。
 *
 * @author Raven
 */
@Slf4j
public class Coordinator implements Lifecycle {

    private static final Object WAKEUP_SIGNAL = new Object();
    private static final int DEFAULT_RESYNC_INTERVAL_MS = 10_000;
    private static final long CONNECTOR_SHUTDOWN_TIMEOUT_MS = 30_000;
    private static final long ELECTION_SHUTDOWN_TIMEOUT_MS = 5_000;

    private final WorkLoop workLoop;
    private final LeaderElection leaderElection;
    private final StreamRegistry streamRegistry;
    private final StreamProvider streamProvider;
    private final SourceRegistry sourceRegistry;
    private final SourceFactory sourceFactory;
    private final TargetRegistry targetRegistry;
    private final SinkFactory sinkFactory;
    private final long connectorShutdownTimeoutMs;
    private final Map<String, RunningSource> runningSources = new LinkedHashMap<>();
    private final Map<String, RunningSink> runningSinks = new LinkedHashMap<>();
    private final Set<String> createdStreams = new HashSet<>();

    public Coordinator(@NonNull LeaderElection leaderElection,
                       @NonNull StreamRegistry streamRegistry,
                       @NonNull StreamProvider streamProvider,
                       @NonNull SourceRegistry sourceRegistry,
                       @NonNull SourceFactory sourceFactory,
                       @NonNull TargetRegistry targetRegistry,
                       @NonNull SinkFactory sinkFactory) {
        this(leaderElection, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, targetRegistry, sinkFactory,
                DEFAULT_RESYNC_INTERVAL_MS);
    }

    public Coordinator(@NonNull LeaderElection leaderElection,
                       @NonNull StreamRegistry streamRegistry,
                       @NonNull StreamProvider streamProvider,
                       @NonNull SourceRegistry sourceRegistry,
                       @NonNull SourceFactory sourceFactory,
                       @NonNull TargetRegistry targetRegistry,
                       @NonNull SinkFactory sinkFactory,
                       int resyncIntervalMs) {
        this(leaderElection, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, targetRegistry, sinkFactory,
                resyncIntervalMs, CONNECTOR_SHUTDOWN_TIMEOUT_MS);
    }

    Coordinator(@NonNull LeaderElection leaderElection,
                @NonNull StreamRegistry streamRegistry,
                @NonNull StreamProvider streamProvider,
                @NonNull SourceRegistry sourceRegistry,
                @NonNull SourceFactory sourceFactory,
                @NonNull TargetRegistry targetRegistry,
                @NonNull SinkFactory sinkFactory,
                int resyncIntervalMs,
                long connectorShutdownTimeoutMs) {
        this.leaderElection = leaderElection;
        this.streamRegistry = streamRegistry;
        this.streamProvider = streamProvider;
        this.sourceRegistry = sourceRegistry;
        this.sourceFactory = sourceFactory;
        this.targetRegistry = targetRegistry;
        this.sinkFactory = sinkFactory;
        this.connectorShutdownTimeoutMs = connectorShutdownTimeoutMs;
        this.workLoop = new WorkLoop("Coordinator", resyncIntervalMs, this::dispatch);
        // 选举事件仅作触发器：入队后由 reconcile 重新读取 isLeader() 再收敛
        this.leaderElection.addListener(event -> {
            log.info("Leadership changed: {}", event);
            this.workLoop.enqueue(event);
        });
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.workLoop.shutdown();
    }

    @Override
    public boolean isAlive() {
        return this.workLoop.isAlive();
    }

    /**
     * 协调器是否处于运行中（含正在停机）：供生命周期装配如实上报状态。
     */
    public boolean isRunning() {
        return this.isAlive();
    }

    public void wake() {
        this.workLoop.enqueue(WAKEUP_SIGNAL);
    }

    private void dispatch(Object message) {
        if (message == WAKEUP_SIGNAL || message instanceof LeaderElection.Event) {
            this.reconcile();
            return;
        }
        if (message instanceof WorkLoopSignal signal) {
            switch (signal) {
                case IDLE -> this.reconcile();
                case STARTED -> this.leaderElection.start();
                case PRE_SHUTDOWN -> this.onPreShutdown();
                case TERMINATED -> this.onTerminated();
            }
            return;
        }
        log.warn("Unhandled message: {}", message);
    }

    /**
     * 水平收敛：先退役（期望中不存在或定义已变更的连接器）并等待关停完成，
     * 再启动缺口中的连接器，避免同名连接器新旧实例并存。
     */
    private void reconcile() {
        List<CompletableFuture<Void>> stops = new ArrayList<>();
        if (!this.leaderElection.isLeader()) {
            this.createdStreams.clear();
            this.retireSources(Map.of(), stops);
            this.retireSinks(Map.of(), stops);
            this.awaitStops(stops);
            return;
        }
        this.reconcileStreams();
        var sources = this.desiredSources();
        var sinks = this.desiredSinks();
        this.retireSources(sources, stops);
        this.retireSinks(sinks, stops);
        this.awaitStops(stops);
        this.startSources(sources);
        this.startSinks(sinks);
    }

    private Map<String, SourceDefinition> desiredSources() {
        return this.sourceRegistry.getSources().stream()
                .filter(SourceDefinition::isEnabled)
                .collect(Collectors.toMap(SourceDefinition::getName, Function.identity(),
                        (a, b) -> a, LinkedHashMap::new));
    }

    private Map<String, TargetDefinition> desiredSinks() {
        return this.targetRegistry.getTargets().stream()
                .filter(TargetDefinition::isEnabled)
                .collect(Collectors.toMap(TargetDefinition::getName, Function.identity(),
                        (a, b) -> a, LinkedHashMap::new));
    }

    private void reconcileStreams() {
        for (var stream : this.streamRegistry.getStreams()) {
            if (this.createdStreams.contains(stream.name())) {
                continue;
            }
            try {
                this.streamProvider.create(stream);
                this.createdStreams.add(stream.name());
            } catch (Exception e) {
                log.error("Failed to create stream '{}', will retry on next reconcile", stream.name(), e);
            }
        }
    }

    private void retireSources(Map<String, SourceDefinition> desired, List<CompletableFuture<Void>> stops) {
        var it = this.runningSources.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var definition = desired.get(entry.getKey());
            boolean inDesired = definition != null && definition.equals(entry.getValue().definition());
            if (inDesired && entry.getValue().connector().isAlive()) {
                continue;
            }
            it.remove();
            if (inDesired) {
                log.info("Source '{}' is not alive, restarting", entry.getKey());
            } else {
                log.info("Source '{}' is out of desired state, stopping", entry.getKey());
            }
            stops.add(entry.getValue().connector().shutdown());
        }
    }

    private void retireSinks(Map<String, TargetDefinition> desired, List<CompletableFuture<Void>> stops) {
        var it = this.runningSinks.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var definition = desired.get(entry.getKey());
            boolean inDesired = definition != null && definition.equals(entry.getValue().definition());
            if (inDesired && entry.getValue().connector().isAlive()) {
                continue;
            }
            it.remove();
            if (inDesired) {
                log.info("Sink '{}' is not alive, restarting", entry.getKey());
            } else {
                log.info("Sink '{}' is out of desired state, stopping", entry.getKey());
            }
            stops.add(entry.getValue().connector().shutdown());
        }
    }

    private void startSources(Map<String, SourceDefinition> desired) {
        for (var definition : desired.values()) {
            if (this.runningSources.containsKey(definition.getName())) {
                continue;
            }
            try {
                var connector = this.sourceFactory.create(this.streamProvider, definition);
                connector.start();
                this.runningSources.put(definition.getName(), new RunningSource(definition, connector));
                log.info("Source '{}' started", definition.getName());
            } catch (Exception e) {
                log.error("Failed to start source '{}', will retry on next reconcile", definition.getName(), e);
            }
        }
    }

    private void startSinks(Map<String, TargetDefinition> desired) {
        for (var definition : desired.values()) {
            if (this.runningSinks.containsKey(definition.getName())) {
                continue;
            }
            try {
                var connector = this.sinkFactory.create(this.streamProvider, definition);
                connector.start();
                this.runningSinks.put(definition.getName(), new RunningSink(definition, connector));
                log.info("Sink '{}' started", definition.getName());
            } catch (Exception e) {
                log.error("Failed to start sink '{}', will retry on next reconcile", definition.getName(), e);
            }
        }
    }

    private void onPreShutdown() {
        List<CompletableFuture<Void>> stops = new ArrayList<>();
        this.retireSources(Map.of(), stops);
        this.retireSinks(Map.of(), stops);
        this.awaitStops(stops);
    }

    private void onTerminated() {
        try {
            this.leaderElection.shutdown().get(ELECTION_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Leader election shutdown interrupted", e);
        } catch (Exception e) {
            log.error("Leader election shutdown failed or timed out", e);
        }
    }

    private void awaitStops(List<CompletableFuture<Void>> stops) {
        if (stops.isEmpty()) {
            return;
        }
        try {
            CompletableFuture.allOf(stops.toArray(CompletableFuture[]::new))
                    .get(this.connectorShutdownTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            log.error("Connector(s) did not stop within {} ms", this.connectorShutdownTimeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Connectors shutdown interrupted", e);
        } catch (Exception e) {
            log.error("Connectors shutdown failed", e);
        }
    }


    private record RunningSource(SourceDefinition definition, SourceConnector connector) {

    }


    private record RunningSink(TargetDefinition definition, SinkConnector connector) {

    }

}
