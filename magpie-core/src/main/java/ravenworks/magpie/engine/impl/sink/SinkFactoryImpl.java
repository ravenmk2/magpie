package ravenworks.magpie.engine.impl.sink;

import java.util.List;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkFactory;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.stream.StreamProvider;


/**
 * @author Raven
 */
@Slf4j
@RequiredArgsConstructor
public class SinkFactoryImpl implements SinkFactory {

    @NonNull
    private final List<SinkProvider> providers;

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        for (var p : this.providers) {
            if (p.type().equals(definition.getType())) {
                return p.create(provider, definition);
            }
        }
        throw new IllegalArgumentException("Unknown target type: " + definition.getType());
    }

}
