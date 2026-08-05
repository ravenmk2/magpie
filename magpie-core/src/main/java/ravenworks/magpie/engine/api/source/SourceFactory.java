package ravenworks.magpie.engine.api.source;

import ravenworks.magpie.engine.api.stream.StreamProducer;


/**
 * @author Raven
 */
public interface SourceFactory {

    SourceConnector create(StreamProducer producer, SourceDefinition definition);

}
