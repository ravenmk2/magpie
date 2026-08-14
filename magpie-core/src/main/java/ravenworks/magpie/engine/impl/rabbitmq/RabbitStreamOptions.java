package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Address;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;


/**
 * RabbitMQ Stream 连接选项：地址与凭据分离配置。URI 中不携带 userinfo，
 * 凭据经 Environment builder 的 username()/password() 传入（URI 缺省 userinfo 时
 * client 自动回落到这两个值）。
 *
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class RabbitStreamOptions implements Serializable {

    private List<URI> uris = List.of(URI.create("rabbitmq-stream://localhost:5552/%2f"));

    private String username = "guest";

    private String password = "guest";

    /**
     * advertised 地址（key）到实际可达地址（value）的映射；空表表示不安装
     * AddressResolver，使用 client 默认解析（原样使用 advertised 地址）
     */
    private Map<Address, Address> addressMappings = Map.of();

}
