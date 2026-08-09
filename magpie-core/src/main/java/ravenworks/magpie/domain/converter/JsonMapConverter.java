package ravenworks.magpie.domain.converter;

import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import ravenworks.magpie.common.json.JsonUtils;

import java.util.Map;


@Converter
public class JsonMapConverter implements AttributeConverter<Map<String, Object>, String> {

    private static final TypeReference<Map<String, Object>> TYPE_REF = new TypeReference<>() {

    };

    @Override
    public String convertToDatabaseColumn(Map<String, Object> attribute) {
        if (attribute == null) {
            return null;
        }
        return JsonUtils.encode(attribute);
    }

    @Override
    public Map<String, Object> convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        return JsonUtils.decode(dbData, TYPE_REF);
    }

}
