package ravenworks.magpie.common.util;

import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * @author Raven
 */
@UtilityClass
public class TimeUtils {

    private static final DateTimeFormatter RFC_3339 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX");

    public static String formatRfc3339(@NonNull LocalDateTime time) {
        ZonedDateTime zdt = time.atZone(ZoneId.systemDefault());
        return zdt.format(RFC_3339);
    }

    public static LocalDateTime parseRfc3339(@NonNull String text) {
        ZonedDateTime zdt = ZonedDateTime.parse(text, RFC_3339);
        return zdt.toLocalDateTime();
    }

}
