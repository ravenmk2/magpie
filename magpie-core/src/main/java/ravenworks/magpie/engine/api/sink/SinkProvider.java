package ravenworks.magpie.engine.api.sink;

import ravenworks.magpie.engine.api.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SinkProvider {

    String type();

    SinkConnector create(StreamProvider provider, TargetDefinition definition);

}
