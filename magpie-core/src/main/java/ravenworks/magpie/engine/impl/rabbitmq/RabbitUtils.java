package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Address;
import lombok.NonNull;
import lombok.experimental.UtilityClass;


/**
 * @author Raven
 */
@UtilityClass
public class RabbitUtils {

    public static String streamQueueName(@NonNull String topicName, int partition) {
        return String.format("magpie-stream.%s-%d", topicName, partition);
    }

    /**
     * 解析 {@code host:port} 形式的地址（按最后一个冒号切分）
     *
     * @throws IllegalArgumentException 缺少端口或端口非数字
     */
    public static Address parseAddress(@NonNull String hostPort) {
        int idx = hostPort.lastIndexOf(':');
        if (idx <= 0 || idx == hostPort.length() - 1) {
            throw new IllegalArgumentException("invalid host:port address: " + hostPort);
        }
        try {
            return new Address(hostPort.substring(0, idx), Integer.parseInt(hostPort.substring(idx + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid host:port address: " + hostPort, e);
        }
    }

}
