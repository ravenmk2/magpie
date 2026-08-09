package ravenworks.magpie.engine.api.source.http;


/**
 * @author Raven
 */
public class TopicNotAllowedException extends RuntimeException {

    public TopicNotAllowedException(String topic) {
        super("Topic not allowed: " + topic);
    }

}
