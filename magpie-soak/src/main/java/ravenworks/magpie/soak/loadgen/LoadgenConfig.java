package ravenworks.magpie.soak.loadgen;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
@Profile("loadgen")
@EnableConfigurationProperties(LoadgenProperties.class)
public class LoadgenConfig {

    @Bean
    public Publisher publisher(LoadgenProperties props, MeterRegistry metrics) {
        return new Publisher(props.getEndpoints(), props.getSource(),
                props.getRequestTimeout(), props.getTopics(), metrics);
    }

    @Bean
    public LoadgenRunner loadgenRunner(LoadgenProperties props, Publisher publisher) {
        return new LoadgenRunner(props, publisher);
    }

    @Bean
    @ConditionalOnProperty(prefix = "soak.loadgen.outbox", name = "enabled", havingValue = "true")
    public OutboxSeeder outboxSeeder(LoadgenProperties props, MeterRegistry metrics) {
        return new OutboxSeeder(props.getOutbox(), metrics);
    }

}
