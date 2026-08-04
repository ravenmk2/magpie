package ravenworks.magpie.engine.lock;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;

import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LeaderLockImplTest {

    /**
     * 手写的 LeaderLockRepository 桩（动态代理）：仅实现本测试用到的方法。
     */
    static class FakeLockRepository {

        LeaderLockEntity lock;
        int renewFailures;
        int renewRows = 1;
        final AtomicInteger findCalls = new AtomicInteger();
        final AtomicInteger renewCalls = new AtomicInteger();

        LeaderLockRepository proxy() {
            return (LeaderLockRepository) Proxy.newProxyInstance(
                    LeaderLockRepository.class.getClassLoader(),
                    new Class<?>[]{LeaderLockRepository.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "findById" -> {
                            this.findCalls.incrementAndGet();
                            yield Optional.ofNullable(this.lock);
                        }
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
                        case "releaseLock" -> 1;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

    }

    @Test
    void renewExceptionReportsLostAndNextPulseReacquires() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy());

        assertEquals(LeaderLock.PulseResult.ACQUIRED, lock.pulse());
        assertEquals(LeaderLock.PulseResult.RENEWED, lock.pulse());

        // 续期抛异常（如 DB 瞬断）：必须报 LOST 并复位内部状态
        repo.renewFailures = 1;
        assertEquals(LeaderLock.PulseResult.LOST, lock.pulse());

        // 下一次 pulse 必须走重新获取路径（findById），而不是继续按持有锁续期；
        // 否则 Coordinator 停止连接器后将永远等不到 ACQUIRED，Leader 空转
        assertEquals(LeaderLock.PulseResult.ACQUIRED, lock.pulse());
        assertEquals(LeaderLock.PulseResult.RENEWED, lock.pulse());

        assertEquals(2, repo.findCalls.get());
        assertEquals(3, repo.renewCalls.get());
    }

    @Test
    void renewWithZeroRowsReportsLostAndReacquiresAfterExpiry() {
        var repo = new FakeLockRepository();
        var lock = new LeaderLockImpl(repo.proxy());

        assertEquals(LeaderLock.PulseResult.ACQUIRED, lock.pulse());
        assertEquals(LeaderLock.PulseResult.RENEWED, lock.pulse());

        // 锁已被其他实例接管：续期影响 0 行
        repo.renewRows = 0;
        assertEquals(LeaderLock.PulseResult.LOST, lock.pulse());

        // 锁行被其他实例持有且未过期：获取失败，且此后不再按持有锁续期
        repo.lock = foreignLock(LocalDateTime.now());
        assertEquals(LeaderLock.PulseResult.FAILED, lock.pulse());
        assertEquals(2, repo.renewCalls.get());

        // 持有者心跳过期后：本实例可接管并恢复 ACQUIRED
        repo.lock.setHeartbeatAt(LocalDateTime.now().minusSeconds(120));
        assertEquals(LeaderLock.PulseResult.ACQUIRED, lock.pulse());
        assertEquals(InstanceId.VALUE, repo.lock.getInstanceId());
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
