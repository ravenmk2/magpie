package ravenworks.magpie.engine.impl.sink.print;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.stream.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Raven
 */
@Slf4j
public class PrintSinkConnector implements SinkConnector {

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final String name;
    private final String topic;
    private final List<PrintSinkWorker> workers = new ArrayList<>();

    public PrintSinkConnector(@NonNull StreamProvider provider,
                              @NonNull StreamRegistry streamRegistry,
                              @NonNull String name,
                              @NonNull String topic) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
        this.name = name;
        this.topic = topic;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        StreamDefinition definition = this.streamRegistry.getStream(this.topic);
        if (definition == null) {
            log.error("Stream not found: {}", this.topic);
            return;
        }
        var consumers = this.provider.consumer(definition, this.name);
        for (int i = 0; i < consumers.size(); i++) {
            var worker = new PrintSinkWorker(this.name, i, consumers.get(i));
            this.workers.add(worker);
            worker.start();
        }
        log.info("Print sink '{}' started, {} worker(s)", this.name, this.workers.size());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        var futures = this.workers.stream()
                .map(PrintSinkWorker::shutdown)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenRun(() -> log.info("Print sink '{}' shutdown", this.name));
    }


    private static class PrintSinkWorker {

        private static final int BATCH_SIZE = 500;
        private static final Object POLL_SIGNAL = new Object();

        private final String name;
        private final int partition;
        private final StreamConsumer consumer;
        private final WorkLoop workLoop;
        private final AtomicLong received = new AtomicLong();
        private volatile boolean stopped;

        PrintSinkWorker(String name, int partition, StreamConsumer consumer) {
            this.name = name;
            this.partition = partition;
            this.consumer = consumer;
            this.workLoop = new WorkLoop("snk-" + name + "-" + partition, 5_000, this::dispatch);
        }

        void start() {
            this.consumer.start();
            this.workLoop.start();
        }

        CompletableFuture<Void> shutdown() {
            this.stopped = true;
            return this.workLoop.shutdown();
        }

        private void dispatch(Object event) {
            if (event == WorkLoopSignal.STARTED) {
                this.workLoop.enqueue(POLL_SIGNAL);
                return;
            }
            if (event == WorkLoopSignal.PRE_SHUTDOWN) {
                try {
                    this.consumer.stop();
                } catch (Exception e) {
                    log.warn("[{}] partition={} error stopping consumer", this.name, this.partition, e);
                }
                return;
            }
            if (this.stopped) {
                return;
            }
            if (event == WorkLoopSignal.IDLE || event == POLL_SIGNAL) {
                this.pollAndProcess();
            }
        }

        private void pollAndProcess() {
            var batch = this.consumer.poll(BATCH_SIZE, Duration.ofMillis(50));
            for (var record : batch) {
                this.log(record);
            }
            if (!batch.isEmpty()) {
                this.consumer.commit(batch.getLast().getOffset());
            }
            if (!this.stopped) {
                this.workLoop.enqueue(POLL_SIGNAL);
            }
        }

        private void log(ConsumerRecord record) {
            long count = this.received.incrementAndGet();
            log.info("[{}] partition={} offset={} count={} payload={}",
                    this.name, this.partition, record.getOffset(), count, new String(record.getMessage().getPayload()));
        }

    }

}
