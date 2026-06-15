package ravenworks.magpie.engine.sink;

import lombok.Data;
import lombok.experimental.Accessors;
import ravenworks.magpie.engine.stream.ConsumerRecord;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class SinkResult {

    private SinkStatus status;
    private int attempts;
    private String error;
    private ConsumerRecord record;

}
