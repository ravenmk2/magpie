package ravenworks.magpie.engine.api.stream;


import java.util.List;


/**
 * @author Raven
 */
public interface StreamProvider extends AutoCloseable {

    void create(StreamDefinition definition);

    StreamProducer producer(StreamDefinition definition);

    List<StreamConsumer> consumer(StreamDefinition definition, String name);

    StreamConsumer consumer(StreamDefinition definition, int partition, String name);

}
