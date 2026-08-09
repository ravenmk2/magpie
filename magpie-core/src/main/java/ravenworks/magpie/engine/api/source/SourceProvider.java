package ravenworks.magpie.engine.api.source;

import ravenworks.magpie.engine.api.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
public interface SourceProvider {

    String type();

    SourceConnector create(StreamProducer producer, String name, Map<String, Object> properties);

}
