package ravenworks.magpie.engine.api.sink;

import ravenworks.magpie.engine.api.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SinkFactory {

    SinkConnector create(StreamProvider provider, TargetDefinition definition);

}
