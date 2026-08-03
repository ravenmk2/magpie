package ravenworks.magpie.common.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


@Slf4j
@UtilityClass
public final class PropertiesUtils {

    private static final ObjectMapper MAPPER = createMapper();

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

    private static ObjectMapper createMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .addHandler(new DeserializationProblemHandler() {

                    @Override
                    public boolean handleUnknownProperty(DeserializationContext ctxt,
                                                         JsonParser p,
                                                         JsonDeserializer<?> deserializer,
                                                         Object bean,
                                                         String propertyName) {
                        log.warn("Unknown property '{}' ignored for {}",
                                propertyName, bean.getClass().getSimpleName());
                        return true;
                    }
                });
    }

}
