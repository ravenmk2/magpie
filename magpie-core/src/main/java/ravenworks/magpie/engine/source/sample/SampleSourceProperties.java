package ravenworks.magpie.engine.source.sample;

import lombok.Data;


/**
 * @author Raven
 */
@Data
public class SampleSourceProperties {

    private String topic;
    private int batchSize = 10;
    private int idleTimeout = 5_000;

}
