package ravenworks.magpie.engine.impl.election;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.engine.api.election.LeaderElection;
import ravenworks.magpie.testsupport.TestJpa;
import ravenworks.magpie.testsupport.TestMySql;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


/**
 * Leader 选举生命周期 IT：真实 MySQL + 真实 repository，验证抢锁、事件上报、
 * 优雅关停放锁、以及放锁后新实例可重新接管。
 *
 * <p>InstanceId.VALUE 是 JVM 级静态值，单 JVM 内无法模拟两个不同实例争抢，
 * 并发抢锁的行锁语义由 {@code LeaderLockRepositoryIT} 覆盖。
 */
class LeaderElectionImplIT {

    private static AnnotationConfigApplicationContext context;
    private static LeaderLockRepository repository;

    @BeforeAll
    static void setUp() {
        context = TestJpa.create(TestMySql.reset());
        repository = context.getBean(LeaderLockRepository.class);
    }

    @AfterAll
    static void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void acquireReleaseAndReacquire() throws Exception {
        List<LeaderElection.Event> events = new CopyOnWriteArrayList<>();

        // 首次启动：心跳节拍内抢锁成功，上报 ACQUIRED，锁行归属本实例
        var election = new LeaderElectionImpl(repository, 50);
        election.addListener(events::add);
        election.start();
        await().atMost(Duration.ofSeconds(5)).until(election::isLeader);
        assertEquals(List.of(LeaderElection.Event.ACQUIRED), events);
        assertEquals(InstanceId.VALUE, repository.findById(1).orElseThrow().getInstanceId());

        // 优雅关停：放锁、锁行置空；按设计不发 LOST（停机由调用方主导）
        election.shutdown().get(5, TimeUnit.SECONDS);
        assertFalse(election.isLeader());
        assertEquals("", repository.findById(1).orElseThrow().getInstanceId());
        assertEquals(List.of(LeaderElection.Event.ACQUIRED), events);

        // 锁已置空：新实例可立即重新接管
        var successor = new LeaderElectionImpl(repository, 50);
        successor.start();
        await().atMost(Duration.ofSeconds(5)).until(successor::isLeader);
        assertEquals(InstanceId.VALUE, repository.findById(1).orElseThrow().getInstanceId());
        successor.shutdown().get(5, TimeUnit.SECONDS);
        assertTrue(events.stream().noneMatch(e -> e == LeaderElection.Event.LOST));
    }

}
