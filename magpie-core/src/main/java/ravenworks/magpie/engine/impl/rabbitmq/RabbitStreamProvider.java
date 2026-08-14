package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamCreator;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.engine.api.stream.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author Raven
 */
@Slf4j
public class RabbitStreamProvider implements StreamProvider {

    private final Environment environment;
    private final OffsetTracker offsetTracker;

    public RabbitStreamProvider(@NonNull RabbitStreamOptions options,
                                @NonNull OffsetTracker offsetTracker) {
        this.offsetTracker = offsetTracker;
        var builder = Environment.builder()
                .id("magpie-" + InstanceId.SHORT)
                .uris(options.getUris().stream().map(URI::toString).toList())
                .username(options.getUsername())
                .password(options.getPassword());
        // 无映射时用 client 默认解析（透传 advertised 地址；localhost+guest 时 client 自动强制
        // localhost 方便本地开发）；有映射时把不可达的 advertised 地址翻译成实际可达地址
        if (!options.getAddressMappings().isEmpty()) {
            builder.addressResolver(new MappedAddressResolver(options.getAddressMappings()));
        }
        this.environment = builder.build();
    }

    @Override
    public void create(@NonNull StreamDefinition definition) {
        log.info("Creating stream {}", definition);
        for (int i = 0; i < definition.partitions(); i++) {
            var partitionName = RabbitUtils.streamQueueName(definition.name(), i);
            this.createStream(partitionName, definition.properties());
        }
    }

    @Override
    public StreamProducer producer(@NonNull StreamDefinition definition) {
        return new RabbitStreamProducer(this.environment, definition);
    }

    @Override
    public List<StreamConsumer> consumer(@NonNull StreamDefinition definition,
                                         @NonNull String name) {
        List<StreamConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < definition.partitions(); i++) {
            consumers.add(new RabbitStreamConsumer(this.environment, definition, i, name, this.offsetTracker));
        }
        return consumers;
    }

    @Override
    public StreamConsumer consumer(@NonNull StreamDefinition definition,
                                   int partition,
                                   @NonNull String name) {
        return new RabbitStreamConsumer(this.environment, definition, partition, name, this.offsetTracker);
    }

    private void createStream(@NonNull String name,
                              @NonNull Map<String, Object> arguments) {
        var creator = this.environment.streamCreator()
                .stream(name);
        arguments.forEach((k, v) -> creator.argument(k, String.valueOf(v)));
        creator.leaderLocator(StreamCreator.LeaderLocator.BALANCED)
                .create();
    }

    @Override
    public void close() {
        log.info("Closing RabbitMQ stream environment");
        this.environment.close();
        log.info("Closed RabbitMQ stream environment");
    }

}
