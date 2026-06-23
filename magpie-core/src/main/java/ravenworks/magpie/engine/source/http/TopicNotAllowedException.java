package ravenworks.magpie.engine.source.http;


/**
 * @author Raven
 */
public class TopicNotAllowedException extends RuntimeException {

    public TopicNotAllowedException(String topic) {
        super("Topic not allowed: " + topic);
    }

}
