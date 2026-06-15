package ravenworks.magpie.engine.sink.http;

import lombok.Data;
import lombok.experimental.Accessors;
import ravenworks.magpie.engine.stream.ConsumerRecord;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class HttpSendResult {

    private DeliverStatus status;
    private int attempts;
    private String error;
    private ConsumerRecord record;

}
