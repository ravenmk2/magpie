package ravenworks.magpie.engine.lock;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;

import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaderLockImplTest {

    /** 测试用心跳间隔：状态迁移应在几十毫秒内发生 */
    private static final long HEARTBEAT_MS = 20;
    private static final long AWAIT_TIMEOUT_MS = 5_000;

    /**
     * 手写的 LeaderLockRepository 桩（动态代理）：仅实现本测试用到的方法。
     * acquireLock 按真实条件 UPDATE 的语义模拟：命中条件才置为持有并返回 1 行。
     */
    static class FakeLockRepository {

        volatile LeaderLockEntity lock;
        int renewFailures;
        int renewRows = 1;
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

    /** pulse() 只上报跳变，反复调用直至等到期望状态（稳态下重复调用无副作用） */
    private static LeaderLock.PulseResult awaitPulse(LeaderLock lock, LeaderLock.PulseResult expected) {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        LeaderLock.PulseResult r = lock.pulse();
        while (r != expected && System.currentTimeMillis() < deadline) {
            LockSupport.parkNanos(5_000_000L);
            r = lock.pulse();
        }
        return r;
    }

    @Test
    void acquireThenRenewViaHeartbeat() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        lock.init();

        // 锁行不存在：首个心跳插入首行并持有
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));
        assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());
        assertEquals(LeaderLock.PulseResult.RENEWED, lock.pulse());

        lock.release();
    }

    @Test
    void renewExceptionReportsLostAndHeartbeatReacquires() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        lock.init();
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));

        // 续期抛异常（如 DB 瞬断）：必须报 LOST 并复位内部状态
        repo.renewFailures = 1;
        assertEquals(LeaderLock.PulseResult.LOST, awaitPulse(lock, LeaderLock.PulseResult.LOST));

        // 心跳线程随后必须重新获取（锁行仍属本实例，条件 UPDATE 直接命中），
        // 否则 Coordinator 停止连接器后将永远等不到 ACQUIRED，Leader 空转
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));
        assertEquals(LeaderLock.PulseResult.RENEWED, lock.pulse());

        lock.release();
    }

    @Test
    void renewWithZeroRowsReportsLostAndReacquiresAfterExpiry() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        lock.init();
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));

        // 锁已被其他实例接管：续期影响 0 行
        repo.lock = foreignLock(LocalDateTime.now());
        repo.renewRows = 0;
        assertEquals(LeaderLock.PulseResult.LOST, awaitPulse(lock, LeaderLock.PulseResult.LOST));

        // 持有者心跳未过期：抢占条件不命中，保持 FAILED，且不再按持有锁续期
        int renewsAtLost = repo.renewCalls.get();
        assertEquals(LeaderLock.PulseResult.FAILED, lock.pulse());
        LockSupport.parkNanos(HEARTBEAT_MS * 3 * 1_000_000L);
        assertEquals(renewsAtLost, repo.renewCalls.get());

        // 持有者心跳过期后：条件 UPDATE 命中，本实例接管并恢复 ACQUIRED
        repo.lock.setHeartbeatAt(LocalDateTime.now().minusSeconds(120));
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));
        assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());

        lock.release();
    }

    @Test
    void releaseStopsHeartbeatAndReleasesLock() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        lock.init();
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));

        lock.release();

        assertEquals(1, repo.releaseCalls.get());
        assertEquals("", repo.lock.getInstanceId());
        // 心跳线程已停：不再有任何续约/获取调用
        int renews = repo.renewCalls.get();
        int acquires = repo.acquireCalls.get();
        LockSupport.parkNanos(HEARTBEAT_MS * 5 * 1_000_000L);
        assertEquals(renews, repo.renewCalls.get());
        assertEquals(acquires, repo.acquireCalls.get());
    }

    @Test
    void releasedLockCanBeAcquiredAgain() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        lock.init();
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(lock, LeaderLock.PulseResult.ACQUIRED));
        int acquiresBefore = repo.acquireCalls.get();
        assertTrue(acquiresBefore > 0);

        lock.release();

        // release 将锁行重置为空锁；随后启动的锁实例应能立即抢占，无需等待过期
        // （本进程内 InstanceId 相同，同实例分支与空锁分支任一命中即成功）
        var other = new LeaderLockImpl(repo.proxy(), HEARTBEAT_MS);
        other.init();
        assertEquals(LeaderLock.PulseResult.ACQUIRED, awaitPulse(other, LeaderLock.PulseResult.ACQUIRED));
        assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());

        other.release();
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
