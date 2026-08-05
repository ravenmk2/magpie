package ravenworks.magpie.engine.impl.stream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.stream.MessageRecord;
import ravenworks.magpie.engine.api.stream.SendResult;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamProducer;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;


/**
 * @author Raven
 */
@Slf4j
public class RoutingStreamProducer implements StreamProducer {

    private final StreamProvider provider;
    private final StreamRegistry registry;
    private final Map<String, StreamProducer> producers = new ConcurrentHashMap<>();

    public RoutingStreamProducer(@NonNull StreamProvider provider,
                                 @NonNull StreamRegistry registry) {
        this.provider = provider;
        this.registry = registry;
    }

    @Override
    public CompletableFuture<SendResult> send(@NonNull MessageRecord record) {
        StreamProducer producer = this.producers.computeIfAbsent(record.getTopic(), topic -> {
            StreamDefinition definition = this.registry.getStream(topic);
            if (definition == null) {
                throw new IllegalArgumentException("Unknown topic: " + topic);
            }
            return this.provider.producer(definition);
        });
        if (producer == null) {
            return CompletableFuture.completedFuture(new SendResult()
                    .setSucceeded(false)
                    .setError("Unknown topic: " + record.getTopic())
                    .setMessage(record));
        }
        return producer.send(record);
    }

    @Override
    public void close() {
        var snapshot = List.copyOf(this.producers.values());
        snapshot.forEach(p -> {
            try {
                p.close();
            } catch (Exception e) {
                log.error("Failed to close producer: {}", e.getMessage());
            }
        });
        this.producers.clear();
    }

}
