package ravenworks.magpie.engine.api.stream;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class ConsumerRecord implements Serializable {

    private long offset;
    private String id;
    private String type;
    private LocalDateTime eventTime;
    private String topic;
    private String tenantId;
    private String businessKey;
    private Map<String, String> headers;
    private byte[] payload;

}
