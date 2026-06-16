package ravenworks.magpie.engine.retry;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;


@Data
@Accessors(chain = true)
public class RetryRecord implements Serializable {

    private String id;
    private String logId;
    private long offset;
    private String messageId;
    private String type;
    private LocalDateTime eventTime;
    private String topic;
    private String tenantId;
    private String businessKey;
    private Map<String, String> headers;
    private byte[] payload;
    private int attempts;
    private LocalDateTime retryAt;

}
