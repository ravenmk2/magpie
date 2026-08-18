package ravenworks.magpie.engine.impl.runtime;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.election.LeaderElection;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkFactory;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.sink.TargetRegistry;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceFactory;
import ravenworks.magpie.engine.api.source.SourceRegistry;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class CoordinatorTest {

    static class FakeLeaderElection implements LeaderElection {

        private final List<Consumer<Event>> listeners = new CopyOnWriteArrayList<>();
        private volatile boolean leader;
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger shutdownCount = new AtomicInteger();

        void setLeader(boolean value) {
            if (this.leader == value) {
                return;
            }
            this.leader = value;
            var event = value ? Event.ACQUIRED : Event.LOST;
            this.listeners.forEach(l -> l.accept(event));
        }

        @Override
        public boolean isLeader() {
            return this.leader;
        }

        @Override
        public void addListener(Consumer<Event> listener) {
            this.listeners.add(listener);
        }

        @Override
        public void start() {
            this.startCount.incrementAndGet();
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            this.shutdownCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isAlive() {
            return true;
        }

    }


    static class FakeConnector implements SourceConnector, SinkConnector {

        private final String name;
        private final String type;
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger shutdownCount = new AtomicInteger();
        volatile boolean hangShutdown;
        private volatile boolean alive = true;

        FakeConnector(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String type() {
            return this.type;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public void start() {
            this.startCount.incrementAndGet();
        }

        @Override
        public CompletableFuture<Void> shutdown() {
            this.shutdownCount.incrementAndGet();
            // 模拟关停卡死：future 永不完成，交由 awaitStops 超时兜底
            return this.hangShutdown ? new CompletableFuture<>() : CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean isAlive() {
            return this.alive;
        }

        /**
         * 模拟连接器静默死亡：配置未变，等待 Coordinator 下轮 reconcile 观测并重建
         */
        void kill() {
            this.alive = false;
        }

    }


    static class FakeStreamProvider implements StreamProvider {

        final List<String> created = new CopyOnWriteArrayList<>();
        final AtomicInteger createFailures = new AtomicInteger();

        @Override
        public void create(StreamDefinition definition) {
            if (this.createFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new RuntimeException("stream creation failed (simulated)");
            }
            this.created.add(definition.name());
        }

        @Override
        public StreamProducer producer(StreamDefinition definition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StreamConsumer> consumer(StreamDefinition definition, String name) {
            return List.of();
        }

        @Override
        public StreamConsumer consumer(StreamDefinition definition, int partition, String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

    }


    static class Harness {

        final FakeLeaderElection election = new FakeLeaderElection();
        final FakeStreamProvider streamProvider = new FakeStreamProvider();
        final AtomicReference<List<SourceDefinition>> sourceDefs = new AtomicReference<>(List.of(
                source("src-on", true), source("src-off", false)));
        final AtomicReference<List<TargetDefinition>> targetDefs = new AtomicReference<>(List.of(
                target("snk-on", true), target("snk-off", false)));
        final AtomicInteger sourceCreateFailures = new AtomicInteger();
        final AtomicInteger sinkCreateFailures = new AtomicInteger();
        final Map<String, FakeConnector> sources = new ConcurrentHashMap<>();
        final Map<String, FakeConnector> sinks = new ConcurrentHashMap<>();
        final List<FakeConnector> allSources = new CopyOnWriteArrayList<>();
        final List<FakeConnector> allSinks = new CopyOnWriteArrayList<>();
        final Coordinator coordinator;

        Harness() {
            this(20, 0);
        }

        Harness(int resyncIntervalMs, long connectorShutdownTimeoutMs) {
            StreamRegistry streamRegistry = new StreamRegistry() {

                @Override
                public List<StreamDefinition> getStreams() {
                    return List.of(new StreamDefinition("s1", 1, Map.of()));
                }

                @Override
                public StreamDefinition getStream(String name) {
                    return null;
                }
            };
            SourceRegistry sourceRegistry = () -> this.sourceDefs.get();
            TargetRegistry targetRegistry = () -> this.targetDefs.get();
            SourceFactory sourceFactory = (producer, definition) -> {
                if (this.sourceCreateFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                    throw new RuntimeException("source creation failed (simulated)");
                }
                var connector = new FakeConnector(definition.getName(), definition.getType());
                this.sources.put(definition.getName(), connector);
                this.allSources.add(connector);
                return connector;
            };
            SinkFactory sinkFactory = (provider, definition) -> {
                if (this.sinkCreateFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                    throw new RuntimeException("sink creation failed (simulated)");
                }
                var connector = new FakeConnector(definition.getName(), definition.getType());
                this.sinks.put(definition.getName(), connector);
                this.allSinks.add(connector);
                return connector;
            };
            // timeoutMs > 0 时走注入超时的构造器，否则走默认构造器保持原有行为
            this.coordinator = connectorShutdownTimeoutMs > 0
                    ? new Coordinator(
                    this.election, streamRegistry, this.streamProvider,
                    sourceRegistry, sourceFactory, targetRegistry, sinkFactory,
                    resyncIntervalMs, connectorShutdownTimeoutMs)
                    : new Coordinator(
                    this.election, streamRegistry, this.streamProvider,
                    sourceRegistry, sourceFactory, targetRegistry, sinkFactory,
                    resyncIntervalMs);
        }

        private static SourceDefinition source(String name, boolean enabled) {
            return source(name, enabled, Map.of());
        }

        private static SourceDefinition source(String name, boolean enabled, Map<String, Object> properties) {
            var definition = new SourceDefinition();
            definition.setName(name);
            definition.setType("sample");
            definition.setEnabled(enabled);
            definition.setProperties(properties);
            return definition;
        }

        private static TargetDefinition target(String name, boolean enabled) {
            var definition = new TargetDefinition();
            definition.setName(name);
            definition.setType("print");
            definition.setTopic("s1");
            definition.setEnabled(enabled);
            definition.setProperties(Map.of());
            return definition;
        }

        void awaitStarted() {
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    this.streamProvider.created.contains("s1")
                            && this.sources.get("src-on") != null && this.sources.get("src-on").startCount.get() == 1
                            && this.sinks.get("snk-on") != null && this.sinks.get("snk-on").startCount.get() == 1);
        }

    }

    @Test
    void leaderStartsStreamsAndConnectors() {
        var h = new Harness();
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            assertFalse(h.sources.containsKey("src-off"));
            assertFalse(h.sinks.containsKey("snk-off"));
            assertEquals(1, h.election.startCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void steadyLeadershipDoesNotRestartConnectors() throws Exception {
        var h = new Harness();
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();
            // 经历多个 resync 节拍后：reconcile 幂等，连接器不重启
            Thread.sleep(200);

            assertEquals(1, h.sources.get("src-on").startCount.get());
            assertEquals(1, h.sinks.get("snk-on").startCount.get());
            assertEquals(0, h.sources.get("src-on").shutdownCount.get());
            assertEquals(0, h.sinks.get("snk-on").shutdownCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void leadershipLostStopsAndReacquiredRestartsConnectors() {
        var h = new Harness();
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            h.election.setLeader(false);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sources.get("src-on").shutdownCount.get() == 1
                            && h.sinks.get("snk-on").shutdownCount.get() == 1);

            h.election.setLeader(true);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.size() == 2 && h.allSources.get(1).startCount.get() == 1
                            && h.allSinks.size() == 2 && h.allSinks.get(1).startCount.get() == 1);
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void streamFailureIsRetriedWithoutBlockingConnectors() {
        var h = new Harness();
        h.streamProvider.createFailures.set(1);
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            // Stream 创建失败不再回滚连接器：连接器照常启动
            h.awaitStarted();
            // Stream 在下轮 reconcile 补齐
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.streamProvider.created.contains("s1"));
            assertEquals(1, h.streamProvider.created.size());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void failingSourceIsRetriedWithoutBlockingSinks() {
        var h = new Harness();
        h.sourceCreateFailures.set(1);
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            // Source 首轮启动失败不影响 Sink；下轮 reconcile 重试成功
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sinks.get("snk-on") != null && h.sinks.get("snk-on").startCount.get() == 1);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sources.get("src-on") != null && h.sources.get("src-on").startCount.get() == 1);
            assertEquals(1, h.allSources.size());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void configChangeRecreatesConnector() {
        var h = new Harness();
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            // 属性变更：连接器重建（旧实例关停、新实例启动）
            h.sourceDefs.set(List.of(
                    Harness.source("src-on", true, Map.of("k", "v")), Harness.source("src-off", false)));
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.size() == 2 && h.allSources.get(0).shutdownCount.get() == 1
                            && h.allSources.get(1).startCount.get() == 1);

            // 禁用：连接器停止且不重建
            h.targetDefs.set(List.of(Harness.target("snk-on", false), Harness.target("snk-off", false)));
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.sinks.get("snk-on").shutdownCount.get() == 1);
            assertEquals(1, h.allSinks.size());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void shutdownStopsConnectorsAndElection() throws Exception {
        var h = new Harness();
        h.coordinator.start();
        h.election.setLeader(true);
        h.awaitStarted();

        h.coordinator.shutdown().get(2, TimeUnit.SECONDS);

        assertEquals(1, h.sources.get("src-on").shutdownCount.get());
        assertEquals(1, h.sinks.get("snk-on").shutdownCount.get());
        assertEquals(1, h.election.shutdownCount.get());
    }

    @Test
    void isRunningReflectsWorkLoopState() throws Exception {
        var h = new Harness();
        assertFalse(h.coordinator.isRunning(), "not running before start");

        h.coordinator.start();
        assertTrue(h.coordinator.isRunning(), "running after start");

        h.coordinator.shutdown().get(2, TimeUnit.SECONDS);
        assertFalse(h.coordinator.isRunning(), "not running after shutdown");
    }

    @Test
    void notLeaderDoesNothing() throws Exception {
        var h = new Harness();
        try {
            h.coordinator.start();
            Thread.sleep(150);

            assertTrue(h.streamProvider.created.isEmpty());
            assertTrue(h.sources.isEmpty());
            assertTrue(h.sinks.isEmpty());
            assertEquals(1, h.election.startCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void failingSinkIsRetriedWithoutBlockingSources() {
        var h = new Harness();
        h.sinkCreateFailures.set(1);
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            // Sink 首轮启动失败不影响 Source；下轮 reconcile 重试成功
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sources.get("src-on") != null && h.sources.get("src-on").startCount.get() == 1);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sinks.get("snk-on") != null && h.sinks.get("snk-on").startCount.get() == 1);
            assertEquals(1, h.allSinks.size());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void wakeTriggersReconcile() {
        // 超长 resync 间隔：IDLE 节拍不会触发，只有 wake 能发起 reconcile
        var h = new Harness(600_000, 0);
        try {
            h.coordinator.start();
            // 直接置位为 leader（不派发选举事件），此时不应有任何收敛动作
            h.election.leader = true;
            assertTrue(h.sources.isEmpty());

            h.coordinator.wake();
            h.awaitStarted();
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void hangingConnectorShutdownTimesOutAndReconcileContinues() {
        // 注入极小关停超时：连接器关停 future 永不完成时 awaitStops 不得卡死循环
        var h = new Harness(600_000, 50);
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            h.allSources.forEach(c -> c.hangShutdown = true);
            h.allSinks.forEach(c -> c.hangShutdown = true);
            // LOST 触发退役：shutdown() 被调用但 future 永不完成，
            // awaitStops 等待 50ms 超时后 reconcile 必须返回（事件仅是触发器，须等退役发生再复位）
            h.election.setLeader(false);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.get(0).shutdownCount.get() == 1
                            && h.allSinks.get(0).shutdownCount.get() == 1);

            // 重新成为 leader：若上轮 reconcile 卡在 awaitStops，连接器将永不重启
            h.election.setLeader(true);
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.size() == 2 && h.allSources.get(1).startCount.get() == 1
                            && h.allSinks.size() == 2 && h.allSinks.get(1).startCount.get() == 1);
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void deadConnectorIsRecreatedWithUnchangedConfig() throws Exception {
        var h = new Harness();
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            // 连接器静默死亡（配置未变）：下轮 reconcile 观测到 isAlive=false，
            // 退役旧实例并以同一期望定义重建
            h.sources.get("src-on").kill();
            h.sinks.get("snk-on").kill();
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.size() == 2 && h.allSources.get(0).shutdownCount.get() == 1
                            && h.allSources.get(1).startCount.get() == 1
                            && h.allSinks.size() == 2 && h.allSinks.get(0).shutdownCount.get() == 1
                            && h.allSinks.get(1).startCount.get() == 1);

            // 新实例健康：后续 reconcile 不抖动、不重建
            h.sources.get("src-on").startCount.set(0);
            Thread.sleep(200);
            assertEquals(0, h.sources.get("src-on").startCount.get());
            assertEquals(0, h.sources.get("src-on").shutdownCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void duplicateNameDefinitionsSilentlyKeepFirst() throws Exception {
        var h = new Harness();
        // 同名定义重复出现：merge (a, b) -> a 静默保留首条、丢弃后者
        var first = Harness.source("src-on", true);
        var second = Harness.source("src-on", true);
        second.setType("other");
        h.sourceDefs.set(List.of(first, second, Harness.source("src-off", false)));
        try {
            h.coordinator.start();
            h.election.setLeader(true);
            h.awaitStarted();

            // 保留的是首条定义（type=sample），且多轮 reconcile 后不抖动、不重建
            assertEquals("sample", h.sources.get("src-on").type());
            Thread.sleep(200);
            assertEquals(1, h.allSources.size());
            assertEquals(1, h.sources.get("src-on").startCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

}
