package ravenworks.magpie.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * @author Raven
 */
@Data
@ConfigurationProperties("magpie.rabbitmq-stream")
public class RabbitStreamProperties implements Serializable {

    private List<URI> uris = List.of(URI.create("rabbitmq-stream://localhost:5552/%2f"));

    private String username = "guest";

    private String password = "guest";

    /**
     * advertised 地址（key，host:port）到实际可达地址（value，host:port）的映射；
     * 空表表示使用 client 默认解析（原样使用 advertised 地址）。
     * 典型场景：集群在 Docker 中，advertised 的容器 hostname 不可达，映射到宿主映射地址
     */
    private Map<String, String> addressMappings = Map.of();

}
