package ravenworks.magpie.engine.impl.source.sample;

import lombok.NonNull;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
public class SampleSourceProvider implements SourceProvider {

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public SourceConnector create(@NonNull StreamProducer producer,
                                  @NonNull String name,
                                  @NonNull Map<String, Object> properties) {
        return new SampleSourceConnector(producer, name, properties);
    }

}
