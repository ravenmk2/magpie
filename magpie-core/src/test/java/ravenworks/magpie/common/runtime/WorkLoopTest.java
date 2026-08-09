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
    void dispatchesStartedEnqueuedAndIdleMessages() {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-dispatch", 20, received::add);
        try {
            loop.start();
            loop.enqueue("hello");

            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    received.contains(WorkLoopSignal.STARTED));
            await().atMost(2, TimeUnit.SECONDS).until(() -> received.contains("hello"));
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    received.contains(WorkLoopSignal.IDLE));
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void messagesEnqueuedBeforeStartAreProcessedAfterStart() {
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
        assertTrue(received.contains(WorkLoopSignal.PRE_SHUTDOWN));
        assertTrue(received.contains(WorkLoopSignal.TERMINATED));
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
        var loop = new WorkLoop("t-throw", 50, message -> {
            if ("boom".equals(message)) {
                throw new RuntimeException("boom");
            }
            received.add(message);
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
    void startTwiceThrows() {
        var loop = new WorkLoop("t-start-twice", 50, e -> {
        });
        try {
            loop.start();
            assertThrows(IllegalStateException.class, loop::start);
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void startAfterShutdownThrows() {
        var loop = new WorkLoop("t-start-after-shutdown", 50, e -> {
        });
        loop.shutdown();
        assertThrows(IllegalStateException.class, loop::start);
    }

    @Test
    void handlerErrorTerminatesLoopExceptionally() {
        var loop = new WorkLoop("t-error", 50, msg -> {
            if ("fatal".equals(msg)) {
                throw new AssertionError("fatal");
            }
        });
        loop.start();
        loop.enqueue("fatal");

        var termination = loop.shutdown();
        await().atMost(2, TimeUnit.SECONDS).until(() -> loop.getState() == WorkLoopState.TERMINATED);
        assertTrue(termination.isCompletedExceptionally());
    }

    @Test
    void queuedMessagesAreDrainedDuringShutdown() throws Exception {
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-drain", 5_000, received::add);
        loop.start();
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                received.contains(WorkLoopSignal.STARTED));

        loop.enqueue("e1");
        loop.enqueue("e2");
        loop.enqueue("e3");
        loop.shutdown().get(2, TimeUnit.SECONDS);

        assertTrue(received.contains("e1"));
        assertTrue(received.contains("e2"));
        assertTrue(received.contains("e3"));
    }

    @Test
    void idleTimeoutBelowFloorIsClampedAndStillDispatches() throws Exception {
        // idleTimeout=0 被构造器钳制到 10ms 下限：循环仍能派发 IDLE，且不会退化成忙轮询
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-idle-floor", 0, received::add);
        try {
            loop.start();
            await().atMost(2, TimeUnit.SECONDS).until(() ->
                    received.contains(WorkLoopSignal.IDLE));

            long before = received.stream().filter(m -> m == WorkLoopSignal.IDLE).count();
            Thread.sleep(300);
            long after = received.stream().filter(m -> m == WorkLoopSignal.IDLE).count();
            // 10ms 下限下 300ms 最多 ~30 次 IDLE；若无下限忙轮询，数量会大出几个数量级
            assertTrue(after - before > 0, "idle dispatch stopped");
            assertTrue(after - before < 1_000, "idle dispatch looks like busy-spin: " + (after - before));
        } finally {
            loop.shutdown();
        }
    }

    @Test
    void noopWakeupSignalIsNeverDelivered() throws Exception {
        // shutdown() 用于唤醒阻塞 poll 的 NOOP 哨兵在 dispatch 中被吞掉，不会投递给 handler
        List<Object> received = new CopyOnWriteArrayList<>();
        var loop = new WorkLoop("t-noop", 5_000, received::add);
        loop.start();
        await().atMost(2, TimeUnit.SECONDS).until(() ->
                received.contains(WorkLoopSignal.STARTED));

        loop.shutdown().get(2, TimeUnit.SECONDS);

        // NOOP 是裸 Object 实例；handler 收到的应只有 WorkLoopSignal 或业务消息
        assertTrue(received.stream().noneMatch(m -> m.getClass() == Object.class),
                "NOOP leaked to handler: " + received);
    }

}
