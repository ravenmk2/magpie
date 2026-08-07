package ravenworks.magpie.common.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class WorkLoopTest {

    @Test
    void shutdownBeforeStartTerminatesImmediately() throws Exception {
        var loop = new WorkLoop("t-shutdown-new", 50, e -> {
        });
        var termination = loop.shutdown();
        termination.get(2, TimeUnit.SECONDS);
        assertEquals(WorkLoopState.TERMINATED, loop.getState());
    }

    @Test
    void dispatchesStartedEnqueuedAndIdleEvents() {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-dispatch", 20, received::add);
        try {
            loop.start();
            loop.enqueue("hello");

            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    received.stream().anyMatch(WorkLoop.Started.class::isInstance));
            await().atMost(2, TimeUnit.SECONDS).until(() -> received.contains("hello"));
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    received.stream().anyMatch(WorkLoop.Idle.class::isInstance));
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void eventsEnqueuedBeforeStartAreProcessedAfterStart() {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-prestart", 50, received::add);
        loop.enqueue("early");
        try {
            loop.start();
            await().atMost(2, TimeUnit.SECONDS).until(() -> received.contains("early"));
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void shutdownDispatchesPreShutdownAndTerminated() throws Exception {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-shutdown", 50, received::add);
        loop.start();
        await().atMost(2, TimeUnit.SECONDS).until(() -> loop.getState() == WorkLoopState.RUNNING);

        var termination = loop.shutdown();
        termination.get(2, TimeUnit.SECONDS);

        assertEquals(WorkLoopState.TERMINATED, loop.getState());
        assertTrue(received.stream().anyMatch(WorkLoop.PreShutdown.class::isInstance));
        assertTrue(received.stream().anyMatch(WorkLoop.Terminated.class::isInstance));
    }

    @Test
    void shutdownTwiceReturnsSameFuture() {
        var loop = new WorkLoop("t-twice", 50, e -> {
        });
        loop.start();
        var first = loop.shutdown();
        var second = loop.shutdown();
        assertSame(first, second);
        first.join();
    }

    @Test
    void enqueueAfterShutdownIsDropped() throws Exception {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-drop", 50, received::add);
        loop.start();
        loop.shutdown().get(2, TimeUnit.SECONDS);

        loop.enqueue("dropped");
        Thread.sleep(100);
        assertFalse(received.contains("dropped"));
    }

    @Test
    void handlerExceptionDoesNotStopLoop() {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-throw", 50, event -> {
            if ("boom".equals(event)) {
                throw new RuntimeException("boom");
            }
            received.add(event);
        });
        try {
            loop.start();
            loop.enqueue("boom");
            loop.enqueue("after");
            await().atMost(2, TimeUnit.SECONDS).until(() -> received.contains("after"));
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void queuedEventsAreDrainedDuringShutdown() throws Exception {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-drain", 5_000, received::add);
        loop.start();
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                received.stream().anyMatch(WorkLoop.Started.class::isInstance));

        loop.enqueue("e1");
        loop.enqueue("e2");
        loop.enqueue("e3");
        loop.shutdown().get(2, TimeUnit.SECONDS);

        assertTrue(received.contains("e1"));
        assertTrue(received.contains("e2"));
        assertTrue(received.contains("e3"));
    }

}
