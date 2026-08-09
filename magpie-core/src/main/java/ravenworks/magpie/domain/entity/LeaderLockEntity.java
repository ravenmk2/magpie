package ravenworks.magpie.domain.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;


@Getter
@Setter
@Entity
@Table(name = "magpie_leader_lock")
public class LeaderLockEntity implements Serializable {

    @Id
    private int id = 1;

    @Column(name = "instance_id", nullable = false, length = 32)
    private String instanceId;

    @Column(name = "acquired_at", nullable = false)
    private LocalDateTime acquiredAt;

    @Column(name = "heartbeat_at", nullable = false)
    private LocalDateTime heartbeatAt;

}
