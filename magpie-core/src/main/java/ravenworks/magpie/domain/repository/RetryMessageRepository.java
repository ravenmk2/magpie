package ravenworks.magpie.domain.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ravenworks.magpie.domain.entity.RetryMessageEntity;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;


public interface RetryMessageRepository extends JpaRepository<RetryMessageEntity, String> {

    @Query("SELECT DISTINCT r.businessKey FROM RetryMessageEntity r WHERE r.consumer = :consumer")
    Set<String> findDistinctBusinessKeysByConsumer(@Param("consumer") String consumer);

    List<RetryMessageEntity> findByConsumerOrderByOffsetAsc(String consumer, Pageable pageable);

    List<RetryMessageEntity> findByConsumerAndRetryAtBeforeOrderByIdAsc(String consumer, LocalDateTime retryAt, Pageable pageable);

}
