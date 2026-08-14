package ravenworks.magpie.engine.impl.source.sample;

import lombok.NonNull;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.stream.RoutingStreamProducer;


/**
 * @author Raven
 */
public class SampleSourceProvider implements SourceProvider {

    private final StreamRegistry streamRegistry;

    public SampleSourceProvider(@NonNull StreamRegistry streamRegistry) {
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public SourceConnector create(@NonNull StreamProvider provider,
                                  @NonNull SourceDefinition definition) {
        var producer = new RoutingStreamProducer(provider, this.streamRegistry);
        return new SampleSourceConnector(producer, definition.getName(), definition.getProperties());
    }

}
