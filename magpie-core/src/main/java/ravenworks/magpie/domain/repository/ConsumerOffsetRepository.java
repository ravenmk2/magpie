package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ravenworks.magpie.domain.entity.ConsumerOffsetEntity;


public interface ConsumerOffsetRepository extends JpaRepository<ConsumerOffsetEntity, String> {

    @Modifying
    @Query(value = "UPDATE magpie_consumer_offset SET `offset` = :offset, version = version + 1 WHERE id = :id", nativeQuery = true)
    int updateOffset(@Param("id") String id, @Param("offset") long offset);

}
