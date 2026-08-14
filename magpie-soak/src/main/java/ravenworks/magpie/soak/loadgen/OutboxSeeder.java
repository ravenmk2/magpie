package ravenworks.magpie.soak.loadgen;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import ravenworks.magpie.soak.probe.ProbeFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Outbox 播种器：向 magpie_outbox_message 表直接插行，覆盖 mysql-poll source 链路。
 * 结构与 LoadgenRunner 相同：每 key 一条虚拟线程链，链内串行、失败重试同一条。
 * 连接断开时整链重连重试（未 COMMIT 的行对 poller 不可见，重发即幂等）。
 */
@Slf4j
public class OutboxSeeder implements SmartLifecycle {

    private static final String INSERT = """
            INSERT INTO magpie_outbox_message
                (id, type, topic, tenant_id, business_key, headers, payload)
            VALUES (?, ?, ?, '', ?, '{}', ?)
            """;

    private final LoadgenProperties.Outbox props;
    private final Counter sentCounter;
    private final Counter errorCounter;
    private final String runId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService executor;

    public OutboxSeeder(LoadgenProperties.Outbox props, MeterRegistry metrics) {
        this.props = props;
        this.sentCounter = metrics.counter("soak.sent", "topic", props.getTopic());
        this.errorCounter = metrics.counter("soak.send.errors", "topic", props.getTopic());
        this.runId = "ox" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
    }

    @Override
    public void start() {
        if (!this.running.compareAndSet(false, true)) {
            return;
        }
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < this.props.getKeyCount(); i++) {
            String key = this.runId + "-k" + i;
            this.executor.submit(() -> this.runChain(key));
        }
        log.info("outbox seeder started: runId={}, topic={}, keys={}, rate={}/s",
                this.runId, this.props.getTopic(), this.props.getKeyCount(), this.props.getRatePerSec());
    }

    private void runChain(String key) {
        long interval = Math.max(1, (long) (this.props.getKeyCount() / this.props.getRatePerSec() * 1000));
        long seq = 0;
        while (this.running.get()) {
            seq++;
            try (var connection = this.connect();
                 PreparedStatement ps = connection.prepareStatement(INSERT)) {
                ps.setString(1, ProbeFactory.newId());
                ps.setString(2, ProbeFactory.EVENT_TYPE);
                ps.setString(3, this.props.getTopic());
                ps.setString(4, key);
                ps.setBytes(5, ProbeFactory.payload(key, seq, 0));
                ps.executeUpdate();
                this.sentCounter.increment();
            } catch (Exception e) {
                this.errorCounter.increment();
                log.warn("outbox insert failed for key={}, retrying same seq: {}", key, e.toString());
                seq--;
                this.sleep(2_000);
                continue;
            }
            this.sleep(interval);
        }
    }

    private Connection connect() throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                var connection = DriverManager.getConnection(
                        this.props.getJdbcUrl(), this.props.getUsername(), this.props.getPassword());
                connection.setAutoCommit(true);
                return connection;
            } catch (Exception e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                this.sleep(1_000);
            }
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        if (this.running.compareAndSet(true, false) && this.executor != null) {
            this.executor.shutdownNow();
            try {
                this.executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("outbox seeder stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

}
