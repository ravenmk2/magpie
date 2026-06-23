package ravenworks.magpie.engine.source.http;

import lombok.NonNull;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.source.SourceProvider;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
public class HttpSourceProvider implements SourceProvider {

    private final HttpSourceRouter router;

    public HttpSourceProvider(@NonNull HttpSourceRouter router) {
        this.router = router;
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public SourceConnector create(@NonNull StreamProducer producer,
                                  @NonNull String name,
                                  @NonNull Map<String, Object> properties) {
        return new HttpSourceConnector(this.router, producer, name, properties);
    }

}
