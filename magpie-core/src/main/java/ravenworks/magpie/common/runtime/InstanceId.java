package ravenworks.magpie.common.runtime;

import lombok.experimental.UtilityClass;
import ravenworks.magpie.common.util.Uuids;


/**
 * @author Raven
 */
@UtilityClass
public final class InstanceId {

    public static final String VALUE = Uuids.uuidHex();

}
