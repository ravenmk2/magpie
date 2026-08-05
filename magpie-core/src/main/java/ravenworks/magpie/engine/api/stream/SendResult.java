package ravenworks.magpie.engine.api.stream;

import lombok.Data;
import lombok.experimental.Accessors;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class SendResult {

    private boolean succeeded;
    private String error;
    private MessageRecord message;

}
