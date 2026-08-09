package ravenworks.magpie.engine.api.sink;

import lombok.Data;
import lombok.experimental.Accessors;
import ravenworks.magpie.engine.api.stream.ConsumerRecord;


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
