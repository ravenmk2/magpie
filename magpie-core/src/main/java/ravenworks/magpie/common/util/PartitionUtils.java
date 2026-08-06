package ravenworks.magpie.common.util;

import com.google.common.hash.Hashing;
import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;


/**
 * @author Raven
 */
@UtilityClass
public class PartitionUtils {

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
