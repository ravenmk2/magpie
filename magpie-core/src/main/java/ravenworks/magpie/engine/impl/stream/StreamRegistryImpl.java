package ravenworks.magpie.engine.impl.stream;

import lombok.NonNull;
import ravenworks.magpie.domain.entity.TopicEntity;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.api.stream.StreamDefinition;
import ravenworks.magpie.engine.api.stream.StreamRegistry;

import java.util.List;


/**
 * @author Raven
 */
public class StreamRegistryImpl implements StreamRegistry {

    private final TopicRepository topicRepository;

    public StreamRegistryImpl(@NonNull TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    @Override
    public List<StreamDefinition> getStreams() {
        return this.topicRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    @Override
    public StreamDefinition getStream(@NonNull String name) {
        return this.topicRepository.findAll()
                .stream()
                .filter(e -> e.getName().equals(name))
                .map(this::toDefinition)
                .findFirst()
                .orElse(null);
    }

    private StreamDefinition toDefinition(@NonNull TopicEntity entity) {
        return new StreamDefinition(
                entity.getName(),
                entity.getPartitions(),
                entity.getProperties()
        );
    }

}
