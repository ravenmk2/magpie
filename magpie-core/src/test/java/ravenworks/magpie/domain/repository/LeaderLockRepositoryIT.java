package ravenworks.magpie.domain.repository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.testsupport.TestJpa;
import ravenworks.magpie.testsupport.TestMySql;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Leader 锁 repository IT：真实 MySQL 上验证条件 UPDATE 的抢锁语义
 * （InnoDB 行锁保证的原子性、空锁释放、心跳过期接管）。
 * 这些语义是 LeaderElectionImpl 正确性的地基，fake 无法验证。
 *
 * <p>两个用例共享一行锁记录、按 @Order 顺序执行（状态机前后衔接）。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LeaderLockRepositoryIT {

    private static AnnotationConfigApplicationContext context;
    private static LeaderLockRepository repository;

    @BeforeAll
    static void setUp() {
        context = TestJpa.create(TestMySql.reset());
        repository = context.getBean(LeaderLockRepository.class);

        // 锁行不存在时条件 UPDATE 不命中，真实流程由 LeaderElectionImpl 插入首行，这里直接播种
        var lock = new LeaderLockEntity();
        lock.setInstanceId("");
        lock.setAcquiredAt(LocalDateTime.now());
        lock.setHeartbeatAt(LocalDateTime.now());
        repository.save(lock);
    }

    @AfterAll
    static void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    @Order(1)
    void lockLifecycle() {
        LocalDateTime now = LocalDateTime.now();

        // 空锁：A 抢得；A 持锁期间 B 抢不到、续约不了、也释放不了
        assertEquals(1, repository.acquireLock("A", now, now.minusSeconds(60)));
        assertEquals(0, repository.acquireLock("B", now, now.minusSeconds(60)));
        assertEquals(0, repository.renewHeartbeat("B", now));
        assertEquals(1, repository.renewHeartbeat("A", now));
        assertEquals(0, repository.releaseLock("B"));

        // A 释放后锁置空，B 可以接管
        assertEquals(1, repository.releaseLock("A"));
        assertEquals("", repository.findById(1).orElseThrow().getInstanceId());
        assertEquals(1, repository.acquireLock("B", now, now.minusSeconds(60)));
        assertEquals("B", repository.findById(1).orElseThrow().getInstanceId());
    }

    @Test
    @Order(2)
    void expiredLockCanBeTakenOver() {
        // 上例结束时 B 持锁且心跳新鲜：C 抢不到
        LocalDateTime now = LocalDateTime.now();
        assertEquals(0, repository.acquireLock("C", now, now.minusSeconds(60)));

        // 模拟 B 进程死亡：心跳停在 5 分钟前，C 凭过期条件接管
        LeaderLockEntity lock = repository.findById(1).orElseThrow();
        lock.setHeartbeatAt(now.minusMinutes(5));
        repository.save(lock);
        assertEquals(1, repository.acquireLock("C", now, now.minusSeconds(60)));
        assertEquals("C", repository.findById(1).orElseThrow().getInstanceId());
    }

}
