package ravenworks.magpie.engine.impl.source;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceFactory;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.StreamProvider;

import java.util.List;


/**
 * @author Raven
 */
@Slf4j
@RequiredArgsConstructor
public class SourceFactoryImpl implements SourceFactory {

    @NonNull
    private final List<SourceProvider> providers;

    @Override
    public SourceConnector create(@NonNull StreamProvider provider,
                                  @NonNull SourceDefinition definition) {
        for (var p : this.providers) {
            if (p.type().equals(definition.getType())) {
                return p.create(provider, definition);
            }
        }
        throw new IllegalArgumentException("Unknown source type: " + definition.getType());
    }

}
