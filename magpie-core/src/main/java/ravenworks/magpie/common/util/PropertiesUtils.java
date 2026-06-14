package ravenworks.magpie.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import lombok.NonNull;

import java.util.Map;


@UtilityClass
public final class PropertiesUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> void bind(@NonNull T target, Map<String, Object> source) {
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
