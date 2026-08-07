package ravenworks.magpie.engine.impl.runtime;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.election.LeaderElection;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.sink.TargetRegistry;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceRegistry;
import ravenworks.magpie.engine.api.stream.*;

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

    }


    static class FakeConnector implements SourceConnector, SinkConnector {

        private final String name;
        private final String type;
        final AtomicInteger startCount = new AtomicInteger();
        final AtomicInteger shutdownCount = new AtomicInteger();

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
            return CompletableFuture.completedFuture(null);
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
        public void close() {
        }

    }


    static class FakeStreamProducer implements StreamProducer {

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            return CompletableFuture.completedFuture(new SendResult().setSucceeded(true).setMessage(record));
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
        final Map<String, FakeConnector> sources = new ConcurrentHashMap<>();
        final Map<String, FakeConnector> sinks = new ConcurrentHashMap<>();
        final List<FakeConnector> allSources = new CopyOnWriteArrayList<>();
        final List<FakeConnector> allSinks = new CopyOnWriteArrayList<>();
        final Coordinator coordinator;

        Harness() {
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
            this.coordinator = new Coordinator(
                    this.election, streamRegistry, this.streamProvider,
                    sourceRegistry,
                    (producer, definition) -> {
                        if (this.sourceCreateFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                            throw new RuntimeException("source creation failed (simulated)");
                        }
                        var connector = new FakeConnector(definition.getName(), definition.getType());
                        this.sources.put(definition.getName(), connector);
                        this.allSources.add(connector);
                        return connector;
                    },
                    targetRegistry,
                    (provider, definition) -> {
                        var connector = new FakeConnector(definition.getName(), definition.getType());
                        this.sinks.put(definition.getName(), connector);
                        this.allSinks.add(connector);
                        return connector;
                    },
                    new FakeStreamProducer(),
                    20);
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

}
