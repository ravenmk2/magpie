package ravenworks.magpie.engine.stream;

import lombok.NonNull;

import java.util.Map;


/**
 * @author Raven
 */
public record StreamDefinition(
        @NonNull String name,
        int partitions,
        Map<String, Object> properties) {

}
