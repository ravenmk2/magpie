package ravenworks.magpie.engine.source.sample;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.util.PropertiesUtils;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class SampleSourceConnector implements SourceConnector {

    private final String name;
    private final String topic;
    private final StreamProducer producer;
    private final int batchSize;
    private final EventLoop eventLoop;

    public SampleSourceConnector(@NonNull StreamProducer producer,
                                 @NonNull String name,
                                 @NonNull Map<String, Object> properties) {
        this.name = name;
        this.producer = producer;
        var props = new SampleSourceProperties();
        PropertiesUtils.bind(props, properties);
        this.topic = props.getTopic() != null && !props.getTopic().isBlank() ? props.getTopic() : name;
        this.batchSize = props.getBatchSize();
        this.eventLoop = new EventLoop("src-" + name, props.getIdleTimeout(), this::dispatch);
    }

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof EventLoop.Idle) {
            this.sendMessages();
        }
    }

    private void sendMessages() {
        for (int i = 0; i < this.batchSize; i++) {
            var msg = new MessageRecord()
                    .setId(Uuids.uuid7Hex())
                    .setType("sample")
                    .setEventTime(LocalDateTime.now())
                    .setTenantId("sample")
                    .setTopic(this.topic)
                    .setBusinessKey(Uuids.uuid7Hex())
                    .setPayload(("Sample message at " + Instant.now()).getBytes(StandardCharsets.UTF_8));
            this.producer.send(msg);
        }
    }

}
