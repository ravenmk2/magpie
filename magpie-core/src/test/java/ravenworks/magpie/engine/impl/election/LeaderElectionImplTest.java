package ravenworks.magpie.engine.impl.election;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.engine.api.election.LeaderElection;

import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class LeaderElectionImplTest {

    /**
     * 测试用心跳间隔：状态迁移应在几十毫秒内发生
     */
    private static final long HEARTBEAT_MS = 20;


    /**
     * 手写的 LeaderLockRepository 桩（动态代理）：仅实现本测试用到的方法。
     * acquireLock 按真实条件 UPDATE 的语义模拟：命中条件才置为持有并返回 1 行。
     */
    static class FakeLockRepository {

        volatile LeaderLockEntity lock;
        volatile int renewFailures;
        volatile int renewRows = 1;
        final AtomicInteger acquireCalls = new AtomicInteger();
        final AtomicInteger renewCalls = new AtomicInteger();
        final AtomicInteger releaseCalls = new AtomicInteger();

        LeaderLockRepository proxy() {
            return (LeaderLockRepository) Proxy.newProxyInstance(
                    LeaderLockRepository.class.getClassLoader(),
                    new Class<?>[]{LeaderLockRepository.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "acquireLock" -> {
                            this.acquireCalls.incrementAndGet();
                            String instanceId = (String) args[0];
                            LocalDateTime now = (LocalDateTime) args[1];
                            LocalDateTime expiry = (LocalDateTime) args[2];
                            LeaderLockEntity current = this.lock;
                            if (current == null) {
                                yield 0;
                            }
                            if (instanceId.equals(current.getInstanceId())
                                    || current.getInstanceId().isEmpty()
                                    || current.getHeartbeatAt().isBefore(expiry)) {
                                current.setInstanceId(instanceId);
                                current.setAcquiredAt(now);
                                current.setHeartbeatAt(now);
                                yield 1;
                            }
                            yield 0;
                        }
                        case "findById" -> Optional.ofNullable(this.lock);
                        case "save" -> {
                            this.lock = (LeaderLockEntity) args[0];
                            yield args[0];
                        }
                        case "renewHeartbeat" -> {
                            this.renewCalls.incrementAndGet();
                            if (this.renewFailures > 0) {
                                this.renewFailures--;
                                throw new RuntimeException("database unavailable (simulated)");
                            }
                            yield this.renewRows;
                        }
                        case "releaseLock" -> {
                            this.releaseCalls.incrementAndGet();
                            this.lock.setInstanceId("");
                            yield 1;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

    }

    @Test
    void acquireThenRenewViaHeartbeat() {
        var repo = new FakeLockRepository();
        var election = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        var events = new CopyOnWriteArrayList<LeaderElection.Event>();
        election.addListener(events::add);
        try {
            election.start();

            // 锁行不存在：首个心跳插入首行并持有
            await().atMost(2, TimeUnit.SECONDS).until(election::isLeader);
            assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());
            assertEquals(List.of(LeaderElection.Event.ACQUIRED), events);

            // 稳态续约：无新事件
            await().atMost(2, TimeUnit.SECONDS).until(() -> repo.renewCalls.get() >= 2);
            assertEquals(List.of(LeaderElection.Event.ACQUIRED), events);
        } finally {
            election.shutdown().join();
        }
    }

    @Test
    void renewExceptionReportsLostAndHeartbeatReacquires() {
        var repo = new FakeLockRepository();
        var election = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        var events = new CopyOnWriteArrayList<LeaderElection.Event>();
        election.addListener(events::add);
        try {
            election.start();
            await().atMost(2, TimeUnit.SECONDS).until(election::isLeader);

            // 续期抛异常（如 DB 瞬断）：必须报 LOST 并复位内部状态。
            // LOST 窗口只有一个心跳间隔（下拍即重抢），断言事件序列而非瞬态
            repo.renewFailures = 1;
            await().atMost(2, TimeUnit.SECONDS).until(() -> events.size() >= 2);
            assertEquals(List.of(LeaderElection.Event.ACQUIRED, LeaderElection.Event.LOST),
                    events.subList(0, 2));

            // 随后必须重新获取（锁行仍属本实例，条件 UPDATE 直接命中），
            // 否则 Coordinator 停止连接器后将永远等不到 ACQUIRED，Leader 空转
            await().atMost(2, TimeUnit.SECONDS).until(() -> events.size() >= 3);
            assertEquals(List.of(LeaderElection.Event.ACQUIRED,
                    LeaderElection.Event.LOST, LeaderElection.Event.ACQUIRED), events);
        } finally {
            election.shutdown().join();
        }
    }

    @Test
    void renewWithZeroRowsReportsLostAndReacquiresAfterExpiry() {
        var repo = new FakeLockRepository();
        var election = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        var events = new CopyOnWriteArrayList<LeaderElection.Event>();
        election.addListener(events::add);
        try {
            election.start();
            await().atMost(2, TimeUnit.SECONDS).until(election::isLeader);

            // 锁已被其他实例接管：续期影响 0 行（LOST 窗口可能只有一拍，断言事件而非瞬态）
            repo.lock = foreignLock(LocalDateTime.now());
            repo.renewRows = 0;
            await().atMost(2, TimeUnit.SECONDS).until(() -> events.size() >= 2);
            assertEquals(LeaderElection.Event.LOST, events.get(1));

            // 持有者心跳未过期：抢占条件不命中，且不再按持有锁续期
            int renewsAtLost = repo.renewCalls.get();
            LockSupport.parkNanos(HEARTBEAT_MS * 3 * 1_000_000L);
            assertEquals(renewsAtLost, repo.renewCalls.get());
            assertFalse(election.isLeader());

            // 持有者心跳过期后：条件 UPDATE 命中，本实例接管并恢复 ACQUIRED。
            // 恢复续约成功（renewRows=1），否则重抢后每拍续约失败会反复抖动
            repo.renewRows = 1;
            repo.lock.setHeartbeatAt(LocalDateTime.now().minusSeconds(120));
            await().atMost(2, TimeUnit.SECONDS).until(() -> events.size() >= 3);
            assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());
            assertEquals(List.of(LeaderElection.Event.ACQUIRED,
                    LeaderElection.Event.LOST, LeaderElection.Event.ACQUIRED), events);
        } finally {
            election.shutdown().join();
        }
    }

    @Test
    void shutdownReleasesLockAndStopsHeartbeat() {
        var repo = new FakeLockRepository();
        var election = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        var events = new CopyOnWriteArrayList<LeaderElection.Event>();
        election.addListener(events::add);
        election.start();
        await().atMost(2, TimeUnit.SECONDS).until(election::isLeader);

        election.shutdown().join();

        assertEquals(1, repo.releaseCalls.get());
        assertEquals("", repo.lock.getInstanceId());
        // 放锁不派发 LOST 事件：停机流程由调用方主导，无需触发收敛
        assertEquals(List.of(LeaderElection.Event.ACQUIRED), events);
        // 循环已停：不再有任何续约/获取调用
        int renews = repo.renewCalls.get();
        int acquires = repo.acquireCalls.get();
        LockSupport.parkNanos(HEARTBEAT_MS * 5 * 1_000_000L);
        assertEquals(renews, repo.renewCalls.get());
        assertEquals(acquires, repo.acquireCalls.get());
    }

    @Test
    void releasedLockCanBeAcquiredAgain() {
        var repo = new FakeLockRepository();
        var election = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        election.start();
        await().atMost(2, TimeUnit.SECONDS).until(election::isLeader);
        assertTrue(repo.acquireCalls.get() > 0);

        election.shutdown().join();

        // release 将锁行重置为空锁；随后启动的选举实例应能立即抢占，无需等待过期
        // （本进程内 InstanceId 相同，同实例分支与空锁分支任一命中即成功）
        var other = new LeaderElectionImpl(repo.proxy(), HEARTBEAT_MS);
        try {
            other.start();
            await().atMost(2, TimeUnit.SECONDS).until(other::isLeader);
            assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());
        } finally {
            other.shutdown().join();
        }
    }

    private static LeaderLockEntity foreignLock(LocalDateTime heartbeatAt) {
        var lock = new LeaderLockEntity();
        lock.setId(1);
        lock.setInstanceId("00000000000000000000000000000000");
        lock.setAcquiredAt(heartbeatAt);
        lock.setHeartbeatAt(heartbeatAt);
        return lock;
    }

}
