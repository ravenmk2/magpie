package ravenworks.magpie.engine.impl.rabbitmq;

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

}
