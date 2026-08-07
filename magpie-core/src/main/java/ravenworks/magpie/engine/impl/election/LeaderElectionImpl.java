package ravenworks.magpie.engine.impl.election;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import ravenworks.magpie.common.runtime.InstanceId;
import ravenworks.magpie.common.runtime.WorkLoop;
import ravenworks.magpie.common.runtime.WorkLoopSignal;
import ravenworks.magpie.domain.entity.LeaderLockEntity;
import ravenworks.magpie.domain.repository.LeaderLockRepository;
import ravenworks.magpie.engine.api.election.LeaderElection;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;


/**
 * @author Raven
 */
@Slf4j
public class LeaderElectionImpl implements LeaderElection {

    private static final Duration LOCK_EXPIRY = Duration.ofSeconds(60);
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;

    private final LeaderLockRepository lockRepository;
    private final WorkLoop workLoop;
    private final List<Consumer<Event>> listeners = new CopyOnWriteArrayList<>();
    /**
     * 锁是否由本实例持有；循环线程写、调用方线程读
     */
    private final AtomicBoolean leader = new AtomicBoolean(false);

    public LeaderElectionImpl(LeaderLockRepository lockRepository) {
        this(lockRepository, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    LeaderElectionImpl(LeaderLockRepository lockRepository, long heartbeatIntervalMs) {
        this.lockRepository = lockRepository;
        this.workLoop = new WorkLoop("LeaderElection", (int) heartbeatIntervalMs, this::dispatch);
    }

    @Override
    public void start() {
        this.workLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.workLoop.shutdown();
    }

    @Override
    public boolean isLeader() {
        return this.leader.get();
    }

    @Override
    public void addListener(Consumer<Event> listener) {
        this.listeners.add(listener);
    }

    private void dispatch(Object message) {
        if (message instanceof WorkLoopSignal signal) {
            switch (signal) {
                case STARTED, IDLE -> this.tick();
                case PRE_SHUTDOWN -> this.releaseLock();
                case TERMINATED -> {
                }
            }
        }
    }

    /**
     * 心跳节拍：持有则续约，未持有则尝试抢占，跳变经监听器上报。
     * 续约/抢占异常（如 DB 瞬断）视为丢锁并复位状态，由后续节拍重新抢占恢复。
     */
    private void tick() {
        try {
            if (this.leader.get()) {
                if (!this.renewInternal()) {
                    log.error("Leader lock renew failed, lock lost");
                    this.setLeader(false);
                }
            } else if (this.acquireInternal()) {
                this.setLeader(true);
            }
        } catch (Exception e) {
            log.error("Leader election heartbeat failed", e);
            this.setLeader(false);
        }
    }

    private void setLeader(boolean value) {
        if (!this.leader.compareAndSet(!value, value)) {
            return;
        }
        var event = value ? Event.ACQUIRED : Event.LOST;
        for (var listener : this.listeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                log.error("Leader election listener failed", e);
            }
        }
    }

    /**
     * 关停时在循环线程内放锁：与 tick 串行，无并发 DB 访问。
     * 不放发 LOST 事件——停机流程由调用方主导，无需再触发收敛。
     */
    private void releaseLock() {
        try {
            this.lockRepository.releaseLock(InstanceId.VALUE);
            log.info("Leader lock released by {}", InstanceId.VALUE);
        } catch (Exception e) {
            log.error("Leader lock release failed", e);
        }
        this.leader.set(false);
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
