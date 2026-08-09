package ravenworks.magpie.engine.impl.source;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.domain.JpaTestSupport;
import ravenworks.magpie.domain.entity.SourceEntity;
import ravenworks.magpie.domain.repository.SourceRepository;
import ravenworks.magpie.engine.api.source.SourceDefinition;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class SourceRegistryImplTest {

    private JpaTestSupport support;
    private SourceRepository repository;
    private SourceRegistryImpl registry;

    @BeforeEach
    void setUp() {
        this.support = JpaTestSupport.create("source-registry-test");
        this.repository = this.support.repository(SourceRepository.class);
        this.registry = new SourceRegistryImpl(this.repository);
    }

    @AfterEach
    void tearDown() {
        this.support.close();
    }

    @Test
    void getSourcesReturnsEmptyListWhenTableIsEmpty() {
        assertTrue(this.registry.getSources().isEmpty());
    }

    @Test
    void mapsEntityFieldsToDefinition() {
        this.repository.save(newSource("source-1", "mysql", "orders-outbox", true,
                Map.of("host", "db.internal", "port", 3306)));

        List<SourceDefinition> sources = this.registry.getSources();

        assertEquals(1, sources.size());
        SourceDefinition def = sources.get(0);
        assertEquals("orders-outbox", def.getName());
        assertEquals("mysql", def.getType());
        assertTrue(def.isEnabled());
        assertEquals(Map.of("host", "db.internal", "port", 3306), def.getProperties());
    }

    @Test
    void mapsAllSourcesIncludingDisabledOnes() {
        this.repository.save(newSource("source-1", "http", "enabled-source", true, Map.of()));
        this.repository.save(newSource("source-2", "sample", "disabled-source", false, Map.of()));

        List<SourceDefinition> sources = this.registry.getSources();

        assertEquals(2, sources.size());
        Map<String, SourceDefinition> byName = sources.stream()
                .collect(Collectors.toMap(SourceDefinition::getName, Function.identity()));
        assertTrue(byName.get("enabled-source").isEnabled());
        SourceDefinition disabled = byName.get("disabled-source");
        assertFalse(disabled.isEnabled());
        assertEquals("sample", disabled.getType());
    }

    private static SourceEntity newSource(String id, String type, String name,
                                          boolean enabled, Map<String, Object> properties) {
        var entity = new SourceEntity();
        entity.setId(id);
        entity.setType(type);
        entity.setName(name);
        entity.setTitle(name + " title");
        entity.setEnabled(enabled);
        entity.setProperties(properties);
        return entity;
    }

}
