package ravenworks.magpie.engine.api.source;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;


/**
 * @author Raven
 */
@Data
public class SourceDefinition implements Serializable {

    private String name;
    private String type;
    private boolean isEnabled = true;
    private Map<String, Object> properties;

}
