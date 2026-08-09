package ravenworks.magpie.domain.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import ravenworks.magpie.domain.converter.JsonMapConverter;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;


@Getter
@Setter
@Entity
@Table(name = "magpie_source")
public class SourceEntity implements Serializable {

    @Id
    @Column(name = "id", nullable = false, length = 32)
    private String id;

    @Column(name = "type", nullable = false, length = 32)
    private String type;

    @Column(name = "name", nullable = false, length = 128)
    private String name;

    @Column(name = "title", nullable = false, length = 128)
    private String title;

    @Column(name = "is_enabled", nullable = false, columnDefinition = "TINYINT(1)")
    private boolean isEnabled;

    @Convert(converter = JsonMapConverter.class)
    @Column(name = "properties", nullable = false)
    private Map<String, Object> properties;

    @Version
    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false, insertable = false, updatable = false)
    private LocalDateTime updatedAt;

}
