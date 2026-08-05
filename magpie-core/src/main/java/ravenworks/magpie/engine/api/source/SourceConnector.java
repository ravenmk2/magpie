package ravenworks.magpie.engine.api.source;

import ravenworks.magpie.common.runtime.Lifecycle;


/**
 * @author Raven
 */
public interface SourceConnector extends Lifecycle {

    String type();

    String name();

}
