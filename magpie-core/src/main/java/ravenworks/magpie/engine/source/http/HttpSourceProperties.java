package ravenworks.magpie.engine.source.http;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Raven
 */
@Data
public class HttpSourceProperties {

    private List<String> allowedTopics = new ArrayList<>();

}
