package ravenworks.magpie.engine.impl.sink;

import java.util.List;
import lombok.NonNull;
import ravenworks.magpie.domain.entity.TargetEntity;
import ravenworks.magpie.domain.repository.TargetRepository;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.sink.TargetRegistry;


/**
 * @author Raven
 */
public class TargetRegistryImpl implements TargetRegistry {

    private final TargetRepository targetRepository;

    public TargetRegistryImpl(@NonNull TargetRepository targetRepository) {
        this.targetRepository = targetRepository;
    }

    @Override
    public List<TargetDefinition> getTargets() {
        return this.targetRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private TargetDefinition toDefinition(TargetEntity entity) {
        var def = new TargetDefinition();
        def.setName(entity.getName());
        def.setType(entity.getType());
        def.setTopic(entity.getTopic());
        def.setEnabled(entity.isEnabled());
        def.setProperties(entity.getProperties());
        return def;
    }

}
