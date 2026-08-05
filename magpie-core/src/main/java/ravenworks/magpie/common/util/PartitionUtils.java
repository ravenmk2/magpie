package ravenworks.magpie.common.util;

import com.google.common.hash.Hashing;
import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;


/**
 * @author Raven
 */
@UtilityClass
public class PartitionUtils {

    /**
     * null 归一为空串：无 businessKey 的消息视为同一条队列（与消费端 keyOf 归一一致），
     * 全部路由到分区 0，在分区内保持相互有序。
     */
    public static int partition(String key, int partitions) {
        if (key == null || key.isEmpty()) {
            return 0;
        }
        int hash = Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt();
        return (hash & Integer.MAX_VALUE) % partitions;
    }

}
