package ravenworks.magpie.engine.impl.sink.http;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.retry.RetryMessageStore;
import ravenworks.magpie.engine.api.sink.SinkConnector;
import ravenworks.magpie.engine.api.sink.SinkProvider;
import ravenworks.magpie.engine.api.sink.TargetDefinition;
import ravenworks.magpie.engine.api.stream.StreamProvider;
import ravenworks.magpie.engine.api.stream.StreamRegistry;


/**
 * @author Raven
 */
@Slf4j
public class HttpSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;
    private final RetryMessageStore retryStore;

    public HttpSinkProvider(@NonNull StreamRegistry streamRegistry,
                            @NonNull RetryMessageStore retryStore) {
        this.streamRegistry = streamRegistry;
        this.retryStore = retryStore;
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        return new HttpSinkConnector(provider, this.streamRegistry, this.retryStore,
                definition.getName(), definition.getTopic(), definition.getProperties());
    }

}
