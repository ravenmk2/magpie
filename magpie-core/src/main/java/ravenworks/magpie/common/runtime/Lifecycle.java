package ravenworks.magpie.common.runtime;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface Lifecycle {

    void start();

    CompletableFuture<Void> shutdown();

}
