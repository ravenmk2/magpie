package ravenworks.magpie.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.LocalDateTime;


@Getter
@Setter
@Entity
@Table(name = "magpie_consumer_offset")
public class ConsumerOffsetEntity implements Serializable {

    @Id
    @Column(name = "id", nullable = false, length = 128)
    private String id;

    @Column(name = "name", nullable = false, length = 128)
    private String name;

    @Column(name = "`partition`", nullable = false)
    private int partition;

    @Column(name = "`offset`", nullable = false)
    private long offset;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime updatedAt;

    public ConsumerOffsetEntity() {
    }

    public ConsumerOffsetEntity(String id, String name, int partition, long offset) {
        this.id = id;
        this.name = name;
        this.partition = partition;
        this.offset = offset;
    }

}
