package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.PartitionUtils;
import ravenworks.magpie.common.util.TimeUtils;
import ravenworks.magpie.engine.stream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class RabbitStreamProducer implements StreamProducer {

    private final List<Producer> producers;

    public RabbitStreamProducer(@NonNull Environment environment,
                                @NonNull StreamDefinition definition) {
        this.producers = new ArrayList<>(definition.partitions());
        for (int i = 0; i < definition.partitions(); i++) {
            String name = RabbitUtils.streamQueueName(definition.name(), i);
            var producer = environment.producerBuilder()
                    .stream(name)
                    .build();
            this.producers.add(producer);
        }
    }

    @Override
    public CompletableFuture<SendResult> send(@NonNull MessageRecord record) {
        int partition = PartitionUtils.partition(record.getBusinessKey(), this.producers.size());
        Producer producer = this.producers.get(partition);
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        var message = buildMessage(producer, record);
        producer.send(message, ctx -> {
            if (ctx.isConfirmed()) {
                future.complete(new SendResult()
                        .setSucceeded(true)
                        .setMessage(record)
                );
            } else {
                future.complete(new SendResult()
                        .setSucceeded(false)
                        .setError("Send failed, code=" + ctx.getCode())
                        .setMessage(record));
            }
        });
        return future;
    }

    private Message buildMessage(@NonNull Producer producer,
                                 @NonNull MessageRecord record) {
        var appProps = producer.messageBuilder()
                .properties()
                .messageId(record.getId())
                .messageBuilder()
                .applicationProperties()
                // null 归一为空串：与路由分区（partition()）及消费端 keyOf 的归一保持一致
                .entry(ReservedHeaders.MSG_BUSINESS_KEY,
                        record.getBusinessKey() != null ? record.getBusinessKey() : "")
                .entry(ReservedHeaders.MSG_TENANT_ID, record.getTenantId())
                .entry(ReservedHeaders.MSG_TYPE, record.getType())
                .entry(ReservedHeaders.MSG_EVENT_TIME, TimeUtils.formatRfc3339(record.getEventTime()));
        if (record.getHeaders() != null) {
            record.getHeaders().forEach(appProps::entry);
        }
        return appProps.messageBuilder()
                .addData(record.getPayload())
                .build();
    }

    @Override
    public void close() {
        this.producers.forEach(p -> {
            try {
                p.close();
            } catch (Exception e) {
                log.error("Failed to close producer: {}", e.getMessage());
            }
        });
        this.producers.clear();
    }

}
