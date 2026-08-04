package ravenworks.magpie.engine.lock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Raven
 */
@Slf4j
@RequiredArgsConstructor
public class LeaderLockImpl implements LeaderLock {

    private static final Duration LOCK_EXPIRY = Duration.ofSeconds(60);

    private final LeaderLockRepository lockRepository;
    private final AtomicBoolean acquired = new AtomicBoolean(false);

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void init() {
        try {
            boolean ok = acquireInternal();
            if (ok) {
                log.info("Leader lock initialized and acquired by {}", InstanceId.VALUE);
            }
        } catch (Exception e) {
            log.error("Leader lock init failed", e);
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public PulseResult pulse() {
        try {
            if (acquired.get()) {
                boolean renewed = renewInternal();
                if (renewed) {
                    return PulseResult.RENEWED;
                }
                acquired.set(false);
                log.error("Leader lock renew failed, lock lost");
                return PulseResult.LOST;
            }
            boolean ok = acquireInternal();
            if (ok) {
                acquired.set(true);
                log.info("Leader lock acquired by {}", InstanceId.VALUE);
                return PulseResult.ACQUIRED;
            }
            return PulseResult.FAILED;
        } catch (Exception e) {
            log.error("Leader lock pulse failed", e);
            // 与续期失败路径保持一致：报 LOST 必须同时复位 acquired，
            // 否则 Coordinator 停止连接器后将永远无法通过 ACQUIRED 恢复
            if (acquired.getAndSet(false)) {
                return PulseResult.LOST;
            }
            return PulseResult.FAILED;
        }
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void release() {
        try {
            lockRepository.releaseLock(InstanceId.VALUE);
            acquired.set(false);
            log.info("Leader lock released by {}", InstanceId.VALUE);
        } catch (Exception e) {
            log.error("Leader lock release failed", e);
        }
    }

    private boolean acquireInternal() {
        var opt = lockRepository.findById(1);
        LocalDateTime now = LocalDateTime.now();
        if (opt.isEmpty()) {
            LeaderLockEntity lock = new LeaderLockEntity();
            lock.setId(1);
            lock.setInstanceId(InstanceId.VALUE);
            lock.setAcquiredAt(now);
            lock.setHeartbeatAt(now);
            lockRepository.save(lock);
            return true;
        }
        LeaderLockEntity lock = opt.get();
        if (InstanceId.VALUE.equals(lock.getInstanceId())) {
            return true;
        }
        LocalDateTime expiry = lock.getHeartbeatAt().plus(LOCK_EXPIRY);
        if (now.isAfter(expiry)) {
            lock.setInstanceId(InstanceId.VALUE);
            lock.setAcquiredAt(now);
            lock.setHeartbeatAt(now);
            lockRepository.save(lock);
            return true;
        }
        return false;
    }

    private boolean renewInternal() {
        int rows = lockRepository.renewHeartbeat(InstanceId.VALUE, LocalDateTime.now());
        return rows > 0;
    }

}
