package ravenworks.magpie.engine.api.stream;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class ConsumerRecord implements Serializable {

    private long offset;
    private MessageRecord message;

}
