package ravenworks.magpie.engine.sink.common;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.util.CircuitBreaker;
import ravenworks.magpie.engine.sink.SinkStatus;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderedSinkWorkerTest {

    private static CircuitBreaker closedCircuit() {
        return new CircuitBreaker("t", 100, 1, 1_000);
    }

    @Test
    void successfulBatchAdvancesOffsetAndCommits() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var worker = new OrderedSinkWorker("w1", consumer, handler, closedCircuit(), 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 1);
            assertTrue(consumer.started.get());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
        assertTrue(consumer.stopped.get());
        assertEquals(1, consumer.lastCommit());
    }

    @Test
    void backoffRetriesSameRecordInPlace() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.BACKOFF);
        var worker = new OrderedSinkWorker("w2", consumer, handler, closedCircuit(), 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> consumer.lastCommit() == 0);
            assertEquals(2, handler.countByOffset(0));
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void failureAbortsBatchWithoutCommit() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        handler.thenReturn(SinkStatus.FAILURE);
        var worker = new OrderedSinkWorker("w3", consumer, handler, closedCircuit(), 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a"), FakeStreamConsumer.record(1, "b")));
            worker.start();

            await().atMost(2, TimeUnit.SECONDS).until(() -> handler.handledRecords.size() == 1);
            Thread.sleep(200);
            assertEquals(1, handler.handledRecords.size());
            assertTrue(consumer.commits.isEmpty());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void openCircuitBreakerPausesProcessing() throws Exception {
        var consumer = new FakeStreamConsumer();
        var handler = new FakeSinkHandler();
        var circuitBreaker = new CircuitBreaker("t", 1, 1, 10_000);
        circuitBreaker.recordFailure();
        var worker = new OrderedSinkWorker("w4", consumer, handler, circuitBreaker, 100);
        try {
            consumer.offer(List.of(FakeStreamConsumer.record(0, "a")));
            worker.start();

            Thread.sleep(250);
            assertTrue(handler.handledRecords.isEmpty());
            assertTrue(consumer.commits.isEmpty());
        } finally {
            worker.shutdown().get(2, TimeUnit.SECONDS);
        }
    }

}
