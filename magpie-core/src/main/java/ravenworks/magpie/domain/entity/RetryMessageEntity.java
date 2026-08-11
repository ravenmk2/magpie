package ravenworks.magpie.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;


@Getter
@Setter
@Entity
@Table(name = "magpie_retry_message")
public class RetryMessageEntity implements Serializable {

    @Id
    @Column(name = "id", nullable = false, length = 32)
    private String id;

    @Column(name = "consumer", nullable = false, length = 128)
    private String consumer;

    @Column(name = "log_id", nullable = false, length = 32)
    private String logId;

    @Column(name = "offset", nullable = false)
    private long offset;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "retry_at", nullable = false)
    private LocalDateTime retryAt;

    @Column(name = "business_key", nullable = false, length = 256)
    private String businessKey;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime updatedAt;

}
