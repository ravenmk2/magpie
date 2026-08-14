package ravenworks.magpie.engine.impl.source.mysql;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.stream.RoutingStreamProducer;


/**
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceProvider implements SourceProvider {

    private final StreamRegistry streamRegistry;

    public MySqlPollSourceProvider(@NonNull StreamRegistry streamRegistry) {
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "mysql-poll";
    }

    @Override
    public SourceConnector create(@NonNull StreamProvider provider,
                                  @NonNull SourceDefinition definition) {
        var producer = new RoutingStreamProducer(provider, this.streamRegistry);
        return new MySqlPollSourceConnector(producer, definition.getName(), definition.getProperties());
    }

}
