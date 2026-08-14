package ravenworks.magpie.server.config;

import lombok.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.engine.api.stream.OffsetTracker;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamOptions;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamProvider;


/**
 * @author Raven
 */
@Configuration
public class StreamProviderConfig {

    @Bean
    private static StreamProvider streamProvider(@NonNull RabbitStreamProperties properties,
                                                 @NonNull OffsetTracker offsetTracker) {
        RabbitStreamOptions options = new RabbitStreamOptions()
                .setUris(properties.getUris())
                .setUsername(properties.getUsername())
                .setPassword(properties.getPassword());
        return new RabbitStreamProvider(options, offsetTracker);
    }

}
