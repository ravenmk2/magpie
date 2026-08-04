package ravenworks.magpie.engine.runtime;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.lock.LeaderLock;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.TargetDefinition;
import ravenworks.magpie.engine.sink.TargetRegistry;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.source.SourceDefinition;
import ravenworks.magpie.engine.source.SourceRegistry;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.SendResult;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProducer;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorTest {

    static class FakeLeaderLock implements LeaderLock {

        private final Queue<PulseResult> script = new ConcurrentLinkedQueue<>();
        final AtomicInteger initCount = new AtomicInteger();
        final AtomicInteger releaseCount = new AtomicInteger();
        final AtomicInteger pulseCount = new AtomicInteger();

        void thenReturn(PulseResult... results) {
            this.script.addAll(List.of(results));
        }

        @Override
        public void init() {
            this.initCount.incrementAndGet();
        }

        @Override
        public PulseResult pulse() {
            this.pulseCount.incrementAndGet();
            var result = this.script.poll();
            return result != null ? result : PulseResult.FAILED;
        }

        @Override
        public void release() {
            this.releaseCount.incrementAndGet();
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

        final FakeLeaderLock lock = new FakeLeaderLock();
        final FakeStreamProvider streamProvider = new FakeStreamProvider();
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
            SourceRegistry sourceRegistry = () -> List.of(
                    source("src-on", true), source("src-off", false));
            TargetRegistry targetRegistry = () -> List.of(
                    target("snk-on", true), target("snk-off", false));
            this.coordinator = new Coordinator(
                    this.lock, streamRegistry, this.streamProvider,
                    sourceRegistry,
                    (producer, definition) -> {
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
            var definition = new SourceDefinition();
            definition.setName(name);
            definition.setType("sample");
            definition.setEnabled(enabled);
            definition.setProperties(Map.of());
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
    void leaderAcquiredInitializesStreamsAndStartsConnectors() {
        var h = new Harness();
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED);
        try {
            h.coordinator.start();
            h.awaitStarted();

            assertFalse(h.sources.containsKey("src-off"));
            assertFalse(h.sinks.containsKey("snk-off"));
            assertEquals(1, h.lock.initCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void leaderRenewedDoesNotRestartConnectors() throws Exception {
        var h = new Harness();
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED,
                LeaderLock.PulseResult.RENEWED, LeaderLock.PulseResult.RENEWED, LeaderLock.PulseResult.RENEWED);
        try {
            h.coordinator.start();
            h.awaitStarted();
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.lock.pulseCount.get() >= 4);
            Thread.sleep(100);

            assertEquals(1, h.sources.get("src-on").startCount.get());
            assertEquals(1, h.sinks.get("snk-on").startCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void leaderReacquiredRestartsConnectors() {
        var h = new Harness();
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED, LeaderLock.PulseResult.LOST,
                LeaderLock.PulseResult.ACQUIRED);
        try {
            h.coordinator.start();
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.allSources.size() == 2 && h.allSources.get(1).startCount.get() == 1
                            && h.allSinks.size() == 2 && h.allSinks.get(1).startCount.get() == 1);

            assertEquals(1, h.allSources.get(0).shutdownCount.get());
            assertEquals(1, h.allSinks.get(0).shutdownCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void leaderRenewedRestartsConnectorsAfterStartupFailure() throws Exception {
        var h = new Harness();
        h.streamProvider.createFailures.set(1);
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED, LeaderLock.PulseResult.RENEWED,
                LeaderLock.PulseResult.RENEWED, LeaderLock.PulseResult.RENEWED);
        try {
            h.coordinator.start();
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    !h.allSources.isEmpty() && h.allSources.get(0).startCount.get() == 1
                            && !h.allSinks.isEmpty() && h.allSinks.get(0).startCount.get() == 1);
            await().atMost(2, TimeUnit.SECONDS).until(() -> h.lock.pulseCount.get() >= 4);
            Thread.sleep(100);

            // 首次 ACQUIRED 启动失败后由 RENEWED 补齐，且后续 RENEWED 不再重复启动
            assertEquals(1, h.allSources.size());
            assertEquals(1, h.allSinks.size());
            assertEquals(1, h.allSources.get(0).startCount.get());
            assertEquals(1, h.allSinks.get(0).startCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void leaderLostStopsConnectors() {
        var h = new Harness();
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED, LeaderLock.PulseResult.LOST);
        try {
            h.coordinator.start();
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.sources.get("src-on") != null && h.sources.get("src-on").shutdownCount.get() == 1
                            && h.sinks.get("snk-on") != null && h.sinks.get("snk-on").shutdownCount.get() == 1);
        } finally {
            h.coordinator.shutdown();
        }
    }

    @Test
    void shutdownStopsConnectorsAndReleasesLock() throws Exception {
        var h = new Harness();
        h.lock.thenReturn(LeaderLock.PulseResult.ACQUIRED, LeaderLock.PulseResult.RENEWED);
        h.coordinator.start();
        h.awaitStarted();

        h.coordinator.shutdown().get(2, TimeUnit.SECONDS);

        assertEquals(1, h.sources.get("src-on").shutdownCount.get());
        assertEquals(1, h.sinks.get("snk-on").shutdownCount.get());
        assertEquals(1, h.lock.releaseCount.get());
    }

    @Test
    void isRunningReflectsEventLoopState() throws Exception {
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
            assertEquals(1, h.lock.initCount.get());
        } finally {
            h.coordinator.shutdown();
        }
    }

}
