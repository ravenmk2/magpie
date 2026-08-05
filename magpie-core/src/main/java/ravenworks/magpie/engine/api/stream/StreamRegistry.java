package ravenworks.magpie.engine.api.stream;


import java.util.List;


/**
 * @author Raven
 */
public interface StreamRegistry {

    List<StreamDefinition> getStreams();

    StreamDefinition getStream(String name);

}
