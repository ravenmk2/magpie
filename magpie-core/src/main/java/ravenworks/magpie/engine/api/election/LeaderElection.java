package ravenworks.magpie.engine.api.election;

import ravenworks.magpie.common.runtime.Lifecycle;

import java.util.function.Consumer;


/**
 * Leader 选举：后台维持选主心跳，领导权跳变通过监听器回调上报。
 * 监听器在选举循环线程上同步调用，必须非阻塞（典型用法：向事件队列入队一个信号，
 * 消费时再调用 {@link #isLeader()} 读取最新状态收敛）。
 *
 * @author Raven
 */
public interface LeaderElection extends Lifecycle {

    boolean isLeader();

    void addListener(Consumer<Event> listener);


    enum Event {
        ACQUIRED,
        LOST
    }

}
