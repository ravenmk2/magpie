package ravenworks.magpie.engine.sink.common;

import ravenworks.magpie.common.runtime.Lifecycle;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface SinkWorker extends Lifecycle {

    void start();

    CompletableFuture<Void> shutdown();

}
