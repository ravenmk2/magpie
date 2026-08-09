package ravenworks.magpie.engine.impl.source;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class SourceFactoryImplTest {

    @Test
    void returnsConnectorFromMatchingProvider() {
        SourceConnector connector = stubConnector("mysql", "source-1");
        var factory = new SourceFactoryImpl(List.of(stubProvider("mysql", connector)));

        SourceConnector created = factory.create(stubStreamProducer(), definition("mysql"));

        assertSame(connector, created);
    }

    @Test
    void forwardsNameAndPropertiesToProvider() {
        var capturedName = new AtomicReference<String>();
        var capturedProperties = new AtomicReference<Map<String, Object>>();
        var provider = new SourceProvider() {
            @Override
            public String type() {
                return "mysql";
            }

            @Override
            public SourceConnector create(StreamProducer producer, String name, Map<String, Object> properties) {
                capturedName.set(name);
                capturedProperties.set(properties);
                return stubConnector("mysql", name);
            }
        };
        var factory = new SourceFactoryImpl(List.of(provider));
        SourceDefinition definition = definition("mysql");
        definition.setProperties(Map.of("table", "outbox"));

        factory.create(stubStreamProducer(), definition);

        assertEquals("source-mysql", capturedName.get());
        assertEquals(Map.of("table", "outbox"), capturedProperties.get());
    }

    @Test
    void throwsWhenNoProviderMatches() {
        var factory = new SourceFactoryImpl(List.of(stubProvider("mysql", stubConnector("mysql", "source-1"))));

        var e = assertThrows(IllegalArgumentException.class,
                () -> factory.create(stubStreamProducer(), definition("http")));

        assertTrue(e.getMessage().contains("http"));
    }

    @Test
    void picksCorrectProviderAmongMultiple() {
        SourceConnector mysqlConnector = stubConnector("mysql", "mysql-source");
        SourceConnector httpConnector = stubConnector("http", "http-source");
        // 记录各 provider 的 create 调用次数，确保只有匹配的 provider 被调用
        var mysqlCreations = new AtomicInteger();
        var httpCreations = new AtomicInteger();
        var factory = new SourceFactoryImpl(List.of(
                stubProvider("mysql", mysqlConnector, mysqlCreations),
                stubProvider("http", httpConnector, httpCreations)));

        SourceConnector created = factory.create(stubStreamProducer(), definition("http"));

        assertSame(httpConnector, created);
        assertEquals(0, mysqlCreations.get());
        assertEquals(1, httpCreations.get());
    }

    private static SourceDefinition definition(String type) {
        var definition = new SourceDefinition();
        definition.setName("source-" + type);
        definition.setType(type);
        return definition;
    }

    private static SourceProvider stubProvider(String type, SourceConnector connector) {
        return stubProvider(type, connector, new AtomicInteger());
    }

    private static SourceProvider stubProvider(String type, SourceConnector connector, AtomicInteger creations) {
        return new SourceProvider() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public SourceConnector create(StreamProducer producer, String name, Map<String, Object> properties) {
                creations.incrementAndGet();
                return connector;
            }
        };
    }

    private static SourceConnector stubConnector(String type, String name) {
        return new SourceConnector() {
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
        };
    }

    private static StreamProducer stubStreamProducer() {
        return new StreamProducer() {
            @Override
            public CompletableFuture<SendResult> send(MessageRecord record) {
                return CompletableFuture.completedFuture(new SendResult()
                        .setSucceeded(true)
                        .setMessage(record));
            }

            @Override
            public void close() {
            }
        };
    }

}
