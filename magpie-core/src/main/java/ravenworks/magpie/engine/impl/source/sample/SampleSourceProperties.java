package ravenworks.magpie.engine.impl.source.sample;

import lombok.Data;


/**
 * @author Raven
 */
@Data
public class SampleSourceProperties {

    private String topic;
    private int batchSize = 10;
    private int interval = 5_000;

}
