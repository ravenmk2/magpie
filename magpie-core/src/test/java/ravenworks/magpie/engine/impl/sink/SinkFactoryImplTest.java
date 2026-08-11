package ravenworks.magpie.engine.impl.sink;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.api.stream.StreamProvider;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


class SinkFactoryImplTest {

    @Test
    void returnsConnectorFromMatchingProvider() {
        SinkConnector connector = stubConnector("http", "sink-1");
        var factory = new SinkFactoryImpl(List.of(stubProvider("http", connector)));

        SinkConnector created = factory.create(stubStreamProvider(), definition("http"));

        assertSame(connector, created);
    }

    @Test
    void throwsWhenNoProviderMatches() {
        var factory = new SinkFactoryImpl(List.of(stubProvider("http", stubConnector("http", "sink-1"))));

        var e = assertThrows(IllegalArgumentException.class,
                () -> factory.create(stubStreamProvider(), definition("jdbc")));

        assertTrue(e.getMessage().contains("jdbc"));
    }

    @Test
    void picksCorrectProviderAmongMultiple() {
        SinkConnector httpConnector = stubConnector("http", "http-sink");
        SinkConnector jdbcConnector = stubConnector("jdbc", "jdbc-sink");
        // 记录各 provider 的 create 调用次数，确保只有匹配的 provider 被调用
        var httpCreations = new AtomicInteger();
        var jdbcCreations = new AtomicInteger();
        var factory = new SinkFactoryImpl(List.of(
                stubProvider("http", httpConnector, httpCreations),
                stubProvider("jdbc", jdbcConnector, jdbcCreations)));

        SinkConnector created = factory.create(stubStreamProvider(), definition("jdbc"));

        assertSame(jdbcConnector, created);
        assertEquals(0, httpCreations.get());
        assertEquals(1, jdbcCreations.get());
    }

    private static TargetDefinition definition(String type) {
        var definition = new TargetDefinition();
        definition.setName("target-" + type);
        definition.setType(type);
        definition.setTopic("orders");
        return definition;
    }

    private static SinkProvider stubProvider(String type, SinkConnector connector) {
        return stubProvider(type, connector, new AtomicInteger());
    }

    private static SinkProvider stubProvider(String type, SinkConnector connector, AtomicInteger creations) {
        return new SinkProvider() {

            @Override
            public String type() {
                return type;
            }

            @Override
            public SinkConnector create(StreamProvider provider, TargetDefinition definition) {
                creations.incrementAndGet();
                return connector;
            }
        };
    }

    private static SinkConnector stubConnector(String type, String name) {
        return new SinkConnector() {

            @Override
            public String type() {
                return type;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public void start() {
            }

            @Override
            public CompletableFuture<Void> shutdown() {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean isAlive() {
                return true;
            }
        };
    }

    private static StreamProvider stubStreamProvider() {
        return new StreamProvider() {

            @Override
            public void create(StreamDefinition definition) {
            }

            @Override
            public StreamProducer producer(StreamDefinition definition) {
                return null;
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
        };
    }

}
