package ravenworks.magpie.engine.api.stream;

import lombok.NonNull;


public interface OffsetTracker {

    long read(@NonNull String name, int partition);

    void write(@NonNull String name, int partition, long offset);

}
