package ravenworks.magpie.engine.impl.source.http;

import lombok.NonNull;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceDefinition;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.source.http.HttpSourceRouter;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;
import ravenworks.magpie.engine.impl.stream.RoutingStreamProducer;


/**
 * @author Raven
 */
public class HttpSourceProvider implements SourceProvider {

    private final HttpSourceRouter router;
    private final StreamRegistry streamRegistry;

    public HttpSourceProvider(@NonNull HttpSourceRouter router,
                              @NonNull StreamRegistry streamRegistry) {
        this.router = router;
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public SourceConnector create(@NonNull StreamProvider provider,
                                  @NonNull SourceDefinition definition) {
        var producer = new RoutingStreamProducer(provider, this.streamRegistry);
        return new HttpSourceConnector(this.router, producer, definition.getName(), definition.getProperties());
    }

}
