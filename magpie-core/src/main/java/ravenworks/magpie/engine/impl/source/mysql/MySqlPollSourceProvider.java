package ravenworks.magpie.engine.impl.source.mysql;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.api.source.SourceConnector;
import ravenworks.magpie.engine.api.source.SourceProvider;
import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
@Slf4j
public class MySqlPollSourceProvider implements SourceProvider {

    @Override
    public String type() {
        return "mysql-poll";
    }

    @Override
    public SourceConnector create(@NonNull StreamProducer producer,
                                  @NonNull String name,
                                  @NonNull Map<String, Object> properties) {
        return new MySqlPollSourceConnector(producer, name, properties);
    }

}
