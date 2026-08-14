package ravenworks.magpie.engine.impl.rabbitmq;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.net.URI;
import java.util.List;


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

}
