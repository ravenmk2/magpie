package ravenworks.magpie.common.runtime;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface Lifecycle {

    void start();

    CompletableFuture<Void> shutdown();

    /**
     * 存活探测：组件是否仍在正常工作。
     * 供调谐循环与健康检查观测实际态；实现必须非阻塞、如实上报，不自愈。
     */
    boolean isAlive();

}
