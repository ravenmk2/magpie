package ravenworks.magpie.server.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;
import ravenworks.magpie.engine.api.lock.LeaderLock;
import ravenworks.magpie.engine.impl.runtime.Coordinator;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkFactory;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.sink.TargetRegistry;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceFactory;
import ravenworks.magpie.engine.api.source.SourceRegistry;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EngineConfigurationTest {

    /**
     * Spring 只停 isRunning() 为 true 的 SmartLifecycle：
     * 该 bean 必须如实上报运行状态，否则优雅停机（释放 Leader 锁、停连接器）不会发生。
     */
    @Test
    void lifecycleTracksRunningStateAndStopsCoordinator() {
        var releaseCount = new AtomicInteger();
        LeaderLock leaderLock = new LeaderLock() {
            @Override
            public void init() {
            }

            @Override
            public PulseResult pulse() {
                return PulseResult.FAILED;
            }

            @Override
            public void release() {
                releaseCount.incrementAndGet();
            }
        };
        StreamRegistry streamRegistry = new StreamRegistry() {
            @Override
            public List<StreamDefinition> getStreams() {
                return List.of();
            }

            @Override
            public StreamDefinition getStream(String name) {
                return null;
            }
        };
        StreamProvider streamProvider = new StreamProvider() {
            @Override
            public void create(StreamDefinition definition) {
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
        };
        SourceRegistry sourceRegistry = List::of;
        SourceFactory sourceFactory = (producer, definition) -> {
            throw new UnsupportedOperationException();
        };
        TargetRegistry targetRegistry = List::of;
        SinkFactory sinkFactory = (provider, definition) -> {
            throw new UnsupportedOperationException();
        };
        StreamProducer sourceProducer = new StreamProducer() {
            @Override
            public CompletableFuture<SendResult> send(MessageRecord record) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        };

        var coordinator = new Coordinator(leaderLock, streamRegistry, streamProvider,
                sourceRegistry, sourceFactory, targetRegistry, sinkFactory, sourceProducer, 10);
        SmartLifecycle lifecycle = EngineConfiguration.coordinatorLifecycle(coordinator);

        assertFalse(lifecycle.isRunning(), "not running before start");

        lifecycle.start();
        assertTrue(lifecycle.isRunning(), "running after start");

        lifecycle.stop();
        assertFalse(lifecycle.isRunning(), "not running after stop");
        assertEquals(1, releaseCount.get(), "stop must shut down the coordinator and release the leader lock");
    }

}
