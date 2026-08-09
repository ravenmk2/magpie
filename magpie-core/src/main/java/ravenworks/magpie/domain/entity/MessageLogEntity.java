package ravenworks.magpie.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import ravenworks.magpie.domain.converter.StringMapConverter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;


@Getter
@Setter
@Entity
@Table(name = "magpie_message_log")
public class MessageLogEntity implements Serializable {

    @Id
    @Column(name = "id", nullable = false, length = 32)
    private String id;

    @Column(name = "message_id", nullable = false, length = 32)
    private String messageId;

    @Column(name = "type", nullable = false, length = 128)
    private String type;

    @Column(name = "event_time", nullable = false)
    private LocalDateTime eventTime;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "tenant_id", nullable = false, length = 64)
    private String tenantId;

    @Column(name = "business_key", nullable = false, length = 128)
    private String businessKey;

    @Column(name = "headers", nullable = false, columnDefinition = "JSON")
    @Convert(converter = StringMapConverter.class)
    private Map<String, String> headers;

    @Column(name = "payload", nullable = false, columnDefinition = "MEDIUMTEXT")
    private String payload;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime createdAt;

}
