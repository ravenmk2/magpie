package ravenworks.magpie.testsupport;

import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.time.Duration;


/**
 * 集成测试共享的 RabbitMQ 容器（Testcontainers singleton），启用 stream 插件。
 * 官方镜像默认不启用 rabbitmq_stream，5552 端口必须在启动前通过
 * withPluginsEnabled 打开。启动超时放宽到 5 分钟：远程共享 Docker 主机负载高时
 * RabbitMQ 首次启动（含插件初始化）可能远超默认的 60s。
 */
public final class TestRabbitMq {

    private static final int STREAM_PORT = 5552;

    private static final RabbitMQContainer CONTAINER =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.3.1-management"))
                    .withExposedPorts(5672, 15672, STREAM_PORT)
                    .withPluginsEnabled("rabbitmq_stream")
                    .withStartupTimeout(Duration.ofMinutes(5));

    static {
        CONTAINER.start();
    }

    private TestRabbitMq() {
    }

    /**
     * rabbitmq-stream:// URI，格式与生产配置 magpie.rabbitmq-stream.uris 一致
     * （见 RabbitStreamProperties 默认值）。
     */
    public static URI streamUri() {
        return URI.create("rabbitmq-stream://" + CONTAINER.getAdminUsername() + ":"
                + CONTAINER.getAdminPassword() + "@" + CONTAINER.getHost() + ":"
                + CONTAINER.getMappedPort(STREAM_PORT) + "/%2f");
    }

}
