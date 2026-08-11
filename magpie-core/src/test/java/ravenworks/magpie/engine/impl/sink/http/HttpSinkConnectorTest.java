package ravenworks.magpie.engine.impl.sink.http;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.retry.RetryRecord;
import ravenworks.magpie.engine.api.stream.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


/**
 * HttpSinkConnector 调谐循环测试：fake StreamProvider/StreamConsumer + 真实 WorkLoop 线程，
 * 覆盖按分区建 worker、worker 死亡原地重建（不连坐其他分区）、
 * stream 缺失后自愈、停机停全部 worker。
 */
class HttpSinkConnectorTest {

    static class FakeStreamConsumer implements StreamConsumer {

        final int partition;
        final AtomicBoolean started = new AtomicBoolean();
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicBoolean failFatally = new AtomicBoolean();

        FakeStreamConsumer(int partition) {
            this.partition = partition;
        }

        @Override
        public int partition() {
            return this.partition;
        }

        @Override
        public void start() {
            this.started.set(true);
        }

        @Override
        public List<ConsumerRecord> poll(int count, Duration timeout) {
            if (this.failFatally.get()) {
                throw new AssertionError("simulated fatal poll failure");
            }
            return List.of();
        }

        @Override
        public void commit(long offset) {
        }

        @Override
        public void stop() {
            this.stopped.set(true);
        }

    }


    static class FakeStreamProvider implements StreamProvider {

        final Map<Integer, List<FakeStreamConsumer>> consumersByPartition = new ConcurrentHashMap<>();

        @Override
        public void create(StreamDefinition definition) {
        }

        @Override
        public StreamProducer producer(StreamDefinition definition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StreamConsumer> consumer(StreamDefinition definition, String name) {
            List<StreamConsumer> result = new ArrayList<>();
            for (int i = 0; i < definition.partitions(); i++) {
                result.add(this.consumer(definition, i, name));
            }
            return result;
        }

        @Override
        public StreamConsumer consumer(StreamDefinition definition, int partition, String name) {
            var consumer = new FakeStreamConsumer(partition);
            this.consumersByPartition.computeIfAbsent(partition, k -> new CopyOnWriteArrayList<>())
                    .add(consumer);
            return consumer;
        }

        List<FakeStreamConsumer> consumers(int partition) {
            return this.consumersByPartition.getOrDefault(partition, List.of());
        }

        @Override
        public void close() {
        }

    }


    static class Harness {

        final FakeStreamProvider provider = new FakeStreamProvider();
        final AtomicReference<StreamDefinition> stream =
                new AtomicReference<>(new StreamDefinition("orders", 2, Map.of()));
        final HttpSinkConnector connector;

        Harness() {
            StreamRegistry streamRegistry = new StreamRegistry() {

                @Override
                public List<StreamDefinition> getStreams() {
                    var s = Harness.this.stream.get();
                    return s == null ? List.of() : List.of(s);
                }

                @Override
                public StreamDefinition getStream(String name) {
                    return Harness.this.stream.get();
                }

            };
            RetryMessageStore retryStore = new RetryMessageStore() {

                @Override
                public Set<String> listKeys(String consumer) {
                    return Set.of();
                }

                @Override
                public List<RetryRecord> list(String consumer, int count) {
                    return List.of();
                }

                @Override
                public List<RetryRecord> listRetryable(String consumer, int count) {
                    return List.of();
                }

                @Override
                public void save(String consumer, ConsumerRecord record) {
                }

                @Override
                public void succeeded(String id) {
                }

                @Override
                public void failed(String id, LocalDateTime retryAt) {
                }

            };
            // 调谐节拍与 worker 停机超时收窄：测试在秒级内完成收敛断言
            this.connector = new HttpSinkConnector(this.provider, streamRegistry, retryStore,
                    "sink-t", "orders", Map.of("url", "http://localhost:9/unreachable"),
                    20, 500);
        }

        void awaitWorkers(int partition, int consumers) {
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    this.provider.consumers(partition).size() == consumers
                            && this.provider.consumers(partition).getLast().started.get());
        }

        void shutdown() throws Exception {
            this.connector.shutdown().get(2, TimeUnit.SECONDS);
        }

    }

    @Test
    void startCreatesWorkerPerPartition() throws Exception {
        var h = new Harness();
        try {
            h.connector.start();

            h.awaitWorkers(0, 1);
            h.awaitWorkers(1, 1);
            assertTrue(h.connector.isAlive());
        } finally {
            h.shutdown();
        }
    }

    @Test
    void deadWorkerIsRecreatedInPlaceWithoutTouchingOthers() throws Exception {
        var h = new Harness();
        try {
            h.connector.start();
            h.awaitWorkers(0, 1);
            h.awaitWorkers(1, 1);

            // partition 0 的 worker 被 Error 杀死：调谐循环应原地重建——
            // 旧 consumer 停止、新 consumer 续建；partition 1 不连坐
            var oldP0 = h.provider.consumers(0).getFirst();
            var p1 = h.provider.consumers(1).getFirst();
            oldP0.failFatally.set(true);

            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    h.provider.consumers(0).size() == 2
                            && h.provider.consumers(0).getLast().started.get());
            assertTrue(oldP0.stopped.get(), "dead worker's consumer is stopped");
            assertEquals(1, h.provider.consumers(1).size(), "healthy partition must not be restarted");
            assertFalse(p1.stopped.get(), "healthy partition keeps running");
            assertTrue(h.connector.isAlive(), "connector stays alive while healing workers");
        } finally {
            h.shutdown();
        }
    }

    @Test
    void missingStreamHealsWhenStreamAppears() throws Exception {
        var h = new Harness();
        h.stream.set(null);
        try {
            h.connector.start();
            // stream 缺失：不建 worker，连接器自身（调谐循环）仍存活
            Thread.sleep(150);
            assertTrue(h.provider.consumersByPartition.isEmpty());
            assertTrue(h.connector.isAlive());

            // stream 出现：下拍 reconcile 自动补齐 worker
            h.stream.set(new StreamDefinition("orders", 2, Map.of()));
            h.awaitWorkers(0, 1);
            h.awaitWorkers(1, 1);
        } finally {
            h.shutdown();
        }
    }

    @Test
    void shutdownStopsAllWorkers() throws Exception {
        var h = new Harness();
        h.connector.start();
        h.awaitWorkers(0, 1);
        h.awaitWorkers(1, 1);

        h.shutdown();

        assertTrue(h.provider.consumers(0).getFirst().stopped.get());
        assertTrue(h.provider.consumers(1).getFirst().stopped.get());
        assertFalse(h.connector.isAlive());
    }

}
