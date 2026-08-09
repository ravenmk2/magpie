package ravenworks.magpie.common.util;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.UUID;


/**
 * @author Raven
 */
@UtilityClass
public class Uuids {

    private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochRandomGenerator();

    public static String uuidHex() {
        return toHex(UUID.randomUUID());
    }

    public static UUID uuid7() {
        return UUID_V7_GENERATOR.generate();
    }

    public static String uuid7Hex() {
        return toHex(uuid7());
    }

    /**
     * 参考: UUID.toString()
     */
    public static String toHex(@NonNull UUID uuid) {
        long mostSigBits = uuid.getMostSignificantBits();
        long leastSigBits = uuid.getLeastSignificantBits();
        return (uuidDigits(mostSigBits >> 32, 8) +
                uuidDigits(mostSigBits >> 16, 4) +
                uuidDigits(mostSigBits, 4) +
                uuidDigits(leastSigBits >> 48, 4) +
                uuidDigits(leastSigBits, 12));
    }

    /**
     * 参考: UUID.toString()
     */
    private static String uuidDigits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

}
