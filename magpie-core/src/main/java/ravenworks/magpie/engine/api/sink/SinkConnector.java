package ravenworks.magpie.engine.api.sink;

import ravenworks.magpie.common.runtime.Lifecycle;


/**
 * @author Raven
 */
public interface SinkConnector extends Lifecycle {

    String type();

    String name();

}
