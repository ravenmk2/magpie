package ravenworks.magpie.engine.stream;

import lombok.NonNull;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.domain.entity.ConsumerOffsetEntity;
import ravenworks.magpie.domain.repository.ConsumerOffsetRepository;


public class OffsetTrackerImpl implements OffsetTracker {

    private final ConsumerOffsetRepository repository;

    public OffsetTrackerImpl(@NonNull ConsumerOffsetRepository repository) {
        this.repository = repository;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public long read(@NonNull String name, int partition) {
        String id = name + ":" + partition;
        var existing = this.repository.findById(id);
        if (existing.isPresent()) {
            return existing.get().getOffset();
        }
        this.repository.save(new ConsumerOffsetEntity(id, name, partition, 0L));
        return 0L;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void write(@NonNull String name, int partition, long offset) {
        String id = name + ":" + partition;
        int rows = this.repository.updateOffset(id, offset);
        if (rows == 0) {
            throw new IllegalStateException(
                    "Consumer offset not found for name=" + name + " partition=" + partition);
        }
    }

}
