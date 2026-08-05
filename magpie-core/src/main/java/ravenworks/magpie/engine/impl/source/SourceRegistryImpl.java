package ravenworks.magpie.engine.impl.source;

import java.util.List;
import lombok.NonNull;
import ravenworks.magpie.domain.entity.SourceEntity;
import ravenworks.magpie.domain.repository.SourceRepository;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceRegistry;


/**
 * @author Raven
 */
public class SourceRegistryImpl implements SourceRegistry {

    private final SourceRepository sourceRepository;

    public SourceRegistryImpl(@NonNull SourceRepository sourceRepository) {
        this.sourceRepository = sourceRepository;
    }

    @Override
    public List<SourceDefinition> getSources() {
        return this.sourceRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private SourceDefinition toDefinition(SourceEntity entity) {
        var def = new SourceDefinition();
        def.setName(entity.getName());
        def.setType(entity.getType());
        def.setEnabled(entity.isEnabled());
        def.setProperties(entity.getProperties());
        return def;
    }

}
