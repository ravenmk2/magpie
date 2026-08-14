package ravenworks.magpie.engine.api.source;

import ravenworks.magpie.engine.api.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SourceProvider {

    String type();

    SourceConnector create(StreamProvider provider, SourceDefinition definition);

}
