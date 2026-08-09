package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.domain.entity.ConsumerOffsetEntity;


public interface ConsumerOffsetRepository extends JpaRepository<ConsumerOffsetEntity, String> {

    /**
     * 修改查询自带事务：OffsetTrackerImpl 经 static @Bean 装配（不受 AOP 代理，方法级
     * @Transactional 不生效），此处的方法级注解是保证 @Modifying 有活动事务的唯一可靠方式
     * （与 LeaderLockRepository 同一约定）。
     */
    @Modifying
    @Transactional
    @Query(value = "UPDATE magpie_consumer_offset SET `offset` = :offset, version = version + 1 WHERE id = :id", nativeQuery = true)
    int updateOffset(@Param("id") String id, @Param("offset") long offset);

}
