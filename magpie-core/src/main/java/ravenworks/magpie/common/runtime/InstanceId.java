package ravenworks.magpie.common.runtime;

import lombok.experimental.UtilityClass;
import ravenworks.magpie.common.util.Uuids;


/**
 * @author Raven
 */
@UtilityClass
public final class InstanceId {

    public static final String VALUE = Uuids.uuidHex();

    /**
     * VALUE 的前 8 位，用于需要实例区分但长度敏感的场景（如连接名前缀）
     */
    public static final String SHORT = VALUE.substring(0, 8);

}
