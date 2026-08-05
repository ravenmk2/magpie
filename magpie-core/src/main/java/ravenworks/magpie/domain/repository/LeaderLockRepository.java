package ravenworks.magpie.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ravenworks.magpie.domain.entity.LeaderLockEntity;

import java.time.LocalDateTime;


public interface LeaderLockRepository extends JpaRepository<LeaderLockEntity, Integer> {

    @Modifying
    @Query(value = "UPDATE magpie_leader_lock SET heartbeat_at = :now WHERE id = 1 AND instance_id = :instanceId", nativeQuery = true)
    int renewHeartbeat(@Param("instanceId") String instanceId, @Param("now") LocalDateTime now);

    /**
     * 原子抢锁：单条条件 UPDATE，命中即持有（InnoDB 行锁保证只有一个事务能命中）。
     * 条件覆盖三种可抢占状态：本实例重取、空锁（release 后 instance_id 置 ''）、心跳过期。
     */
    @Modifying
    @Query(value = "UPDATE magpie_leader_lock SET instance_id = :instanceId, acquired_at = :now, heartbeat_at = :now"
            + " WHERE id = 1 AND (instance_id = :instanceId OR instance_id = '' OR heartbeat_at < :expiry)",
            nativeQuery = true)
    int acquireLock(@Param("instanceId") String instanceId, @Param("now") LocalDateTime now,
                    @Param("expiry") LocalDateTime expiry);

    @Modifying
    @Query(value = "UPDATE magpie_leader_lock SET instance_id = '', acquired_at = '1970-01-01 00:00:00.000', heartbeat_at = '1970-01-01 00:00:00.000' WHERE id = 1 AND instance_id = :instanceId", nativeQuery = true)
    int releaseLock(@Param("instanceId") String instanceId);

}
