package ravenworks.magpie.testkit.loadgen;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import ravenworks.magpie.testkit.probe.ProbeFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 负载发生器主循环：每 (topic, key) 一条虚拟线程链，链内严格串行——
 * 同 key 前一条确认后才发下一条，保证探针在 stream 中的顺序可预期
 * （跨实例故障转移时的不确定窗口只会产生重复，不会产生新序号越过旧序号）。
 * 每次启动生成新 runId 作为 key 前缀，重启后序列从零开始，verifier 按新 key 建状态。
 */
@Slf4j
public class LoadgenRunner implements SmartLifecycle {

    private final LoadgenProperties props;
    private final Publisher publisher;
    private final String runId;
    private final int chainCount;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final long startedAt = System.currentTimeMillis();
    private ExecutorService executor;

    public LoadgenRunner(LoadgenProperties props, Publisher publisher) {
        this.props = props;
        this.publisher = publisher;
        this.runId = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        this.chainCount = props.getTopics().size() * props.getKeyCount();
    }

    @Override
    public void start() {
        if (!this.running.compareAndSet(false, true)) {
            return;
        }
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        for (String topic : this.props.getTopics()) {
            for (int i = 0; i < this.props.getKeyCount(); i++) {
                String key = this.runId + "-k" + i;
                this.executor.submit(() -> this.runChain(topic, key));
            }
        }
        log.info("loadgen started: runId={}, topics={}, keys/topic={}, rate={}/s",
                this.runId, this.props.getTopics(), this.props.getKeyCount(), this.props.getRatePerSec());
    }

    private void runChain(String topic, String key) {
        long seq = 0;
        while (this.running.get()) {
            seq++;
            byte[] body = ProbeFactory.cloudEvent(topic, key, seq, this.props.getPayloadSize());
            while (this.running.get() && !this.publisher.send(topic, body)) {
                this.sleep(this.props.getRetryDelay().toMillis());
            }
            this.sleep(this.nextIntervalMillis());
        }
    }

    /**
     * 每链间隔 = 链数 / 当前速率；峰值窗口内速率乘倍率
     */
    private long nextIntervalMillis() {
        double rate = this.props.getRatePerSec();
        var burst = this.props.getBurst();
        if (!burst.getEvery().isZero()) {
            long cycle = (System.currentTimeMillis() - this.startedAt) % burst.getEvery().toMillis();
            if (cycle < burst.getDuration().toMillis()) {
                rate *= burst.getMultiplier();
            }
        }
        return Math.max(1, (long) (this.chainCount / rate * 1000));
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
            log.info("loadgen stopped");
        }
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

}
