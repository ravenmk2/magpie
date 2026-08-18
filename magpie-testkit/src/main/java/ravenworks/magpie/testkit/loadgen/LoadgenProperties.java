package ravenworks.magpie.testkit.loadgen;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


@Data
@ConfigurationProperties("testkit.loadgen")
public class LoadgenProperties {

    /**
     * magpie-server 实例地址列表，按序故障转移（publish 只会在运行 source 连接器的实例上成功）
     */
    private List<String> endpoints = new ArrayList<>(List.of("http://localhost:8080"));

    /**
     * 发布目标 source 名（对应 magpie_source 中的 http source 注册名）
     */
    private String source = "testkit-http";

    /**
     * 负载覆盖的 topic 列表，每 topic 独立 keyspace
     */
    private List<String> topics = new ArrayList<>(List.of("testkit-ordered", "testkit-key-ordered", "testkit-best-effort"));

    /**
     * 每 topic 的 key 数；总链数 = topics × keyCount
     */
    private int keyCount = 40;

    /**
     * 全局目标速率（msg/s，所有链合计）
     */
    private double ratePerSec = 100;

    private int payloadSize = 256;

    private Duration requestTimeout = Duration.ofSeconds(10);

    /**
     * 发送失败后的重试间隔（同一条 seq 重试，不前进）
     */
    private Duration retryDelay = Duration.ofSeconds(2);

    private Burst burst = new Burst();

    private Outbox outbox = new Outbox();


    @Data
    public static class Burst {

        /**
         * 峰值周期；0 表示关闭峰值，恒定速率
         */
        private Duration every = Duration.ZERO;

        private Duration duration = Duration.ofMinutes(2);

        /**
         * 峰值期速率倍率
         */
        private double multiplier = 5;

    }


    @Data
    public static class Outbox {

        /**
         * 是否启用 MySQL outbox 播种链路（覆盖 mysql-poll source）
         */
        private boolean enabled = false;

        private String jdbcUrl = "jdbc:mysql://localhost:3306/magpie?useSSL=false&characterEncoding=UTF-8";

        private String username = "root";

        private String password = "root";

        private String topic = "testkit-outbox";

        private int keyCount = 20;

        private double ratePerSec = 20;

    }

}
