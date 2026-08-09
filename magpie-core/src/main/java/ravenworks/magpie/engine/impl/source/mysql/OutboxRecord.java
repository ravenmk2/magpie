package ravenworks.magpie.engine.impl.source.mysql;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;


@Data
public class OutboxRecord {

    private String id;
    private String type;
    private LocalDateTime eventTime;
    private String topic;
    private String tenantId;
    private String businessKey;
    private Map<String, String> headers;
    private String payload;
    private LocalDateTime createdAt;

}
