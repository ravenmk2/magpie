package ravenworks.magpie.engine.impl.stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.domain.JpaTestSupport;
import ravenworks.magpie.domain.entity.ConsumerOffsetEntity;
import ravenworks.magpie.domain.repository.ConsumerOffsetRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class OffsetTrackerImplTest {

    private JpaTestSupport support;
    private ConsumerOffsetRepository repository;
    private OffsetTrackerImpl tracker;

    @BeforeEach
    void setUp() {
        this.support = JpaTestSupport.create("offset-tracker-test");
        this.repository = this.support.repository(ConsumerOffsetRepository.class);
        this.tracker = new OffsetTrackerImpl(this.repository);
    }

    @AfterEach
    void tearDown() {
        this.support.close();
    }

    @Test
    void readOnMissingRowInsertsInitialRowAndReturnsMinusOne() {
        long offset = this.tracker.read("printer", 0);

        assertEquals(-1L, offset);
        var row = this.repository.findById("printer:0");
        assertTrue(row.isPresent());
        ConsumerOffsetEntity entity = row.get();
        assertEquals("printer", entity.getName());
        assertEquals(0, entity.getPartition());
        assertEquals(-1L, entity.getOffset());
    }

    @Test
    void readReturnsStoredOffset() {
        this.tracker.read("printer", 1);
        this.tracker.write("printer", 1, 42L);

        assertEquals(42L, this.tracker.read("printer", 1));
    }

    @Test
    void writeUpdatesExistingRow() {
        this.tracker.read("printer", 2);

        this.tracker.write("printer", 2, 100L);

        assertEquals(100L, this.repository.findById("printer:2").orElseThrow().getOffset());
    }

    @Test
    void writeOnMissingRowThrowsIllegalState() {
        var exception = assertThrows(IllegalStateException.class,
                () -> this.tracker.write("ghost", 0, 1L));
        assertTrue(exception.getMessage().contains("ghost"));
    }

    @Test
    void offsetsAreTrackedPerNameAndPartition() {
        this.tracker.read("a", 0);
        this.tracker.write("a", 0, 7L);

        assertEquals(-1L, this.tracker.read("a", 1));
        assertEquals(-1L, this.tracker.read("b", 0));
        assertEquals(7L, this.tracker.read("a", 0));
    }

}
