package ravenworks.magpie.domain.converter;

import com.fasterxml.jackson.core.type.TypeReference;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import ravenworks.magpie.common.json.JsonUtils;

import java.util.Map;


@Converter
public class StringMapConverter implements AttributeConverter<Map<String, String>, String> {

    private static final TypeReference<Map<String, String>> TYPE_REF = new TypeReference<>() {

    };

    @Override
    public String convertToDatabaseColumn(Map<String, String> attribute) {
        if (attribute == null) {
            return null;
        }
        return JsonUtils.encode(attribute);
    }

    @Override
    public Map<String, String> convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        return JsonUtils.decode(dbData, TYPE_REF);
    }

}
