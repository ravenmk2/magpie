package ravenworks.magpie.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.Map;


@UtilityClass
public final class PropertiesUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void bind(@NonNull Object target, Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return;
        }
        try {
            MAPPER.updateValue(target, source);
        } catch (Exception e) {
            throw new RuntimeException("Failed to bind properties to " + target.getClass().getSimpleName(), e);
        }
    }

}
