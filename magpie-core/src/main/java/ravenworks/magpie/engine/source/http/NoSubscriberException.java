package ravenworks.magpie.engine.source.http;


/**
 * @author Raven
 */
public class NoSubscriberException extends RuntimeException {

    public NoSubscriberException(String source) {
        super("No active HTTP source subscriber for: " + source);
    }

}
