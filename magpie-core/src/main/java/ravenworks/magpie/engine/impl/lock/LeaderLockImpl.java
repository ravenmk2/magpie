package ravenworks.magpie.engine.impl.lock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.engine.api.lock.LeaderLock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;


/**
 * @author Raven
 */
@Slf4j
public class LeaderLockImpl implements LeaderLock {

    private static final Duration LOCK_EXPIRY = Duration.ofSeconds(60);
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;

    private final LeaderLockRepository lockRepository;
    private final long heartbeatIntervalMs;
    /**
     * 锁是否由本实例持有；由心跳线程写、调用方线程读
     */
    private final AtomicBoolean acquired = new AtomicBoolean(false);
    /**
     * pulse() 上次上报的持有状态，用于推导 ACQUIRED/LOST 跳变
     */
    private final AtomicBoolean reported = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Thread heartbeatThread;

    public LeaderLockImpl(LeaderLockRepository lockRepository) {
        this(lockRepository, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    LeaderLockImpl(LeaderLockRepository lockRepository, long heartbeatIntervalMs) {
        this.lockRepository = lockRepository;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    /**
     * 启动独立心跳线程：锁的获取/续约不再依赖 Coordinator 的 pulse 节奏，
     * 关停连接器期间（事件循环阻塞）心跳照常，锁不会因停摆而过期被抢。
     */
    @Override
    public void init() {
        if (this.running.compareAndSet(false, true)) {
            this.heartbeatThread = Thread.ofVirtual()
                    .name("leader-lock-heartbeat")
                    .start(this::heartbeatLoop);
        }
    }

    /**
     * 只读心跳线程维护的持有状态并上报跳变，本身不做任何 IO。
     * 语义：false→true 报 ACQUIRED，持续持有报 RENEWED，true→false 报 LOST，持续不持有报 FAILED。
     */
    @Override
    public PulseResult pulse() {
        boolean now = this.acquired.get();
        boolean prev = this.reported.getAndSet(now);
        if (now) {
            return prev ? PulseResult.RENEWED : PulseResult.ACQUIRED;
        }
        return prev ? PulseResult.LOST : PulseResult.FAILED;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void release() {
        this.running.set(false);
        Thread t = this.heartbeatThread;
        if (t != null) {
            LockSupport.unpark(t);
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.heartbeatThread = null;
        }
        try {
            this.lockRepository.releaseLock(InstanceId.VALUE);
            this.acquired.set(false);
            log.info("Leader lock released by {}", InstanceId.VALUE);
        } catch (Exception e) {
            log.error("Leader lock release failed", e);
        }
    }

    private void heartbeatLoop() {
        while (this.running.get()) {
            tick();
            LockSupport.parkNanos(this.heartbeatIntervalMs * 1_000_000L);
        }
    }

    private void tick() {
        try {
            if (this.acquired.get()) {
                if (!renewInternal()) {
                    this.acquired.set(false);
                    log.error("Leader lock renew failed, lock lost");
                }
            } else if (acquireInternal()) {
                // release() 可能在本 tick 的 DB 调用期间发生：已停则放弃本次获取结果
                if (this.running.get()) {
                    this.acquired.set(true);
                    log.info("Leader lock acquired by {}", InstanceId.VALUE);
                }
            }
        } catch (Exception e) {
            // 与续期失败路径保持一致：异常（如 DB 瞬断）视为丢锁并复位持有状态，
            // 否则 Coordinator 停止连接器后将永远无法通过 ACQUIRED 恢复
            log.error("Leader lock heartbeat failed", e);
            this.acquired.set(false);
        }
    }

    /**
     * 原子抢占（条件 UPDATE）；锁行不存在时插入首行，
     * 并发插入时 PK 冲突的一方本轮失败，下个心跳走条件 UPDATE 自愈。
     */
    private boolean acquireInternal() {
        LocalDateTime now = LocalDateTime.now();
        int rows = this.lockRepository.acquireLock(InstanceId.VALUE, now, now.minus(LOCK_EXPIRY));
        if (rows > 0) {
            return true;
        }
        if (this.lockRepository.findById(1).isPresent()) {
            return false;
        }
        try {
            LeaderLockEntity lock = new LeaderLockEntity();
            lock.setId(1);
            lock.setInstanceId(InstanceId.VALUE);
            lock.setAcquiredAt(now);
            lock.setHeartbeatAt(now);
            this.lockRepository.save(lock);
            return true;
        } catch (DataIntegrityViolationException e) {
            return false;
        }
    }

    private boolean renewInternal() {
        int rows = this.lockRepository.renewHeartbeat(InstanceId.VALUE, LocalDateTime.now());
        return rows > 0;
    }

}
