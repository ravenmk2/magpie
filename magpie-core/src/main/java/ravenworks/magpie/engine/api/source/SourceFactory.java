package ravenworks.magpie.engine.api.source;

import ravenworks.magpie.engine.api.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SourceFactory {

    SourceConnector create(StreamProvider provider, SourceDefinition definition);

}
