package ravenworks.magpie.engine.source.http;


/**
 * @author Raven
 */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message) {
        super(message);
    }

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
