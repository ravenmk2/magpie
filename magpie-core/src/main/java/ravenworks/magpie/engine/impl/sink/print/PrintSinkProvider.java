package ravenworks.magpie.engine.impl.sink.print;

import lombok.NonNull;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;


/**
 * @author Raven
 */
public class PrintSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;

    public PrintSinkProvider(@NonNull StreamRegistry streamRegistry) {
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        return new PrintSinkConnector(provider, this.streamRegistry,
                definition.getName(), definition.getTopic());
    }

}
