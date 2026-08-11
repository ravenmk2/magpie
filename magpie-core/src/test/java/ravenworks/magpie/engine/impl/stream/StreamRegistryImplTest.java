package ravenworks.magpie.engine.impl.stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.domain.JpaTestSupport;
import ravenworks.magpie.domain.entity.TopicEntity;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.api.stream.StreamDefinition;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


class StreamRegistryImplTest {

    private JpaTestSupport support;
    private TopicRepository repository;
    private StreamRegistryImpl registry;

    @BeforeEach
    void setUp() {
        this.support = JpaTestSupport.create("stream-registry-test");
        this.repository = this.support.repository(TopicRepository.class);
        this.registry = new StreamRegistryImpl(this.repository);
    }

    @AfterEach
    void tearDown() {
        this.support.close();
    }

    @Test
    void getStreamsReturnsEmptyListWhenTableIsEmpty() {
        assertTrue(this.registry.getStreams().isEmpty());
    }

    @Test
    void getStreamsMapsEntityFieldsToDefinitions() {
        this.repository.save(newTopic("topic-1", "orders", 8, Map.of("retentionDays", 7)));

        List<StreamDefinition> streams = this.registry.getStreams();

        assertEquals(1, streams.size());
        StreamDefinition def = streams.get(0);
        assertEquals("orders", def.name());
        assertEquals(8, def.partitions());
        assertEquals(Map.of("retentionDays", 7), def.properties());
    }

    @Test
    void getStreamByNameReturnsMatchingDefinition() {
        this.repository.save(newTopic("topic-1", "orders", 8, Map.of()));
        this.repository.save(newTopic("topic-2", "audit", 2, Map.of()));

        StreamDefinition def = this.registry.getStream("audit");

        assertEquals("audit", def.name());
        assertEquals(2, def.partitions());
        assertEquals(Map.of(), def.properties());
    }

    @Test
    void getStreamReturnsNullForUnknownName() {
        this.repository.save(newTopic("topic-1", "orders", 8, Map.of()));

        assertNull(this.registry.getStream("missing"));
    }

    @Test
    void getStreamsReturnsAllTopics() {
        this.repository.save(newTopic("topic-1", "orders", 8, Map.of()));
        this.repository.save(newTopic("topic-2", "audit", 2, Map.of()));

        List<StreamDefinition> streams = this.registry.getStreams();

        assertEquals(2, streams.size());
        Set<String> names = streams.stream().map(StreamDefinition::name).collect(Collectors.toSet());
        assertEquals(Set.of("orders", "audit"), names);
    }

    private static TopicEntity newTopic(String id, String name, int partitions,
                                        Map<String, Object> properties) {
        var entity = new TopicEntity();
        entity.setId(id);
        entity.setName(name);
        entity.setTitle(name + " title");
        entity.setPartitions(partitions);
        entity.setProperties(properties);
        return entity;
    }

}
