package ravenworks.magpie.engine.impl.stream;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamConsumer;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class RoutingStreamProducerTest {

    @Test
    void cachesProducerPerTopic() {
        var provider = new FakeStreamProvider();
        var routing = new RoutingStreamProducer(provider, registryWith("orders"));

        routing.send(new MessageRecord().setTopic("orders"));
        routing.send(new MessageRecord().setTopic("orders"));

        // 同一 topic 只创建一次底层 producer
        assertEquals(1, provider.creations);
        assertEquals(2, provider.created.get(0).sendCount);
    }

    @Test
    void createsSeparateProducersForDifferentTopics() {
        var provider = new FakeStreamProvider();
        var routing = new RoutingStreamProducer(provider, registryWith("orders", "audit"));

        routing.send(new MessageRecord().setTopic("orders"));
        routing.send(new MessageRecord().setTopic("audit"));

        assertEquals(2, provider.creations);
    }

    @Test
    void sendPropagatesIllegalArgumentExceptionForUnknownTopic() {
        var provider = new FakeStreamProvider();
        var routing = new RoutingStreamProducer(provider, registryWith("orders"));

        // computeIfAbsent 的映射函数直接抛出，异常原样传播给调用方
        var e = assertThrows(IllegalArgumentException.class,
                () -> routing.send(new MessageRecord().setTopic("missing")));
        assertTrue(e.getMessage().contains("missing"));
        assertEquals(0, provider.creations);
    }

    @Test
    void closeClosesAllProducersEvenWhenOneThrows() {
        var failing = new RecordingProducer();
        failing.throwOnClose = true;
        var normal = new RecordingProducer();
        var provider = new FakeStreamProvider(failing, normal);
        var routing = new RoutingStreamProducer(provider, registryWith("orders", "audit"));
        routing.send(new MessageRecord().setTopic("orders"));
        routing.send(new MessageRecord().setTopic("audit"));

        // 某个 producer 关闭失败不影响其余的关闭，close 本身不抛出
        assertDoesNotThrow(routing::close);
        assertEquals(1, failing.closeCount);
        assertEquals(1, normal.closeCount);
    }

    private static StreamRegistry registryWith(String... topics) {
        Map<String, StreamDefinition> streams = new HashMap<>();
        for (String topic : topics) {
            streams.put(topic, new StreamDefinition(topic, 1, Map.of()));
        }
        return new StreamRegistry() {
            @Override
            public List<StreamDefinition> getStreams() {
                return List.copyOf(streams.values());
            }

            @Override
            public StreamDefinition getStream(String name) {
                return streams.get(name);
            }
        };
    }

    /** 记录 producer 创建次数的假 StreamProvider，可按需指定要派发的 producer */
    private static final class FakeStreamProvider implements StreamProvider {

        private final Deque<RecordingProducer> queue = new ArrayDeque<>();
        private final List<RecordingProducer> created = new ArrayList<>();
        private int creations;

        FakeStreamProvider(RecordingProducer... producers) {
            this.queue.addAll(List.of(producers));
        }

        @Override
        public void create(StreamDefinition definition) {
        }

        @Override
        public StreamProducer producer(StreamDefinition definition) {
            this.creations++;
            RecordingProducer producer = this.queue.isEmpty() ? new RecordingProducer() : this.queue.poll();
            this.created.add(producer);
            return producer;
        }

        @Override
        public List<StreamConsumer> consumer(StreamDefinition definition, String name) {
            return List.of();
        }

        @Override
        public void close() {
        }
    }

    /** 记录 send/close 调用次数的假 StreamProducer，可配置 close 时抛出异常 */
    private static final class RecordingProducer implements StreamProducer {

        private int sendCount;
        private int closeCount;
        private boolean throwOnClose;

        @Override
        public CompletableFuture<SendResult> send(MessageRecord record) {
            this.sendCount++;
            return CompletableFuture.completedFuture(new SendResult()
                    .setSucceeded(true)
                    .setMessage(record));
        }

        @Override
        public void close() {
            this.closeCount++;
            if (this.throwOnClose) {
                throw new RuntimeException("close failed");
            }
        }
    }

}
