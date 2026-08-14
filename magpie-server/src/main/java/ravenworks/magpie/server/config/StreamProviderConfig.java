package ravenworks.magpie.server.config;

import com.rabbitmq.stream.Address;
import lombok.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.engine.api.stream.OffsetTracker;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamOptions;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitStreamProvider;
import ravenworks.magpie.engine.impl.rabbitmq.RabbitUtils;

import java.util.HashMap;
import java.util.Map;


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
                .setPassword(properties.getPassword())
                .setAddressMappings(parseAddressMappings(properties.getAddressMappings()));
        return new RabbitStreamProvider(options, offsetTracker);
    }

    private static Map<Address, Address> parseAddressMappings(@NonNull Map<String, String> mappings) {
        Map<Address, Address> parsed = new HashMap<>();
        mappings.forEach((advertised, reachable) -> parsed.put(
                RabbitUtils.parseAddress(advertised), RabbitUtils.parseAddress(reachable)));
        return parsed;
    }

}
