package ravenworks.magpie.engine.impl.sink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ravenworks.magpie.domain.JpaTestSupport;
import ravenworks.magpie.domain.entity.TargetEntity;
import ravenworks.magpie.domain.repository.TargetRepository;
import ravenworks.magpie.engine.api.sink.TargetDefinition;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class TargetRegistryImplTest {

    private JpaTestSupport support;
    private TargetRepository repository;
    private TargetRegistryImpl registry;

    @BeforeEach
    void setUp() {
        this.support = JpaTestSupport.create("target-registry-test");
        this.repository = this.support.repository(TargetRepository.class);
        this.registry = new TargetRegistryImpl(this.repository);
    }

    @AfterEach
    void tearDown() {
        this.support.close();
    }

    @Test
    void getTargetsReturnsEmptyListWhenTableIsEmpty() {
        assertTrue(this.registry.getTargets().isEmpty());
    }

    @Test
    void mapsEntityFieldsToDefinition() {
        this.repository.save(newTarget("target-1", "http", "audit-sink", "orders", true,
                Map.of("url", "http://example.com/hook", "batchSize", 50)));

        List<TargetDefinition> targets = this.registry.getTargets();

        assertEquals(1, targets.size());
        TargetDefinition def = targets.get(0);
        assertEquals("audit-sink", def.getName());
        assertEquals("http", def.getType());
        assertEquals("orders", def.getTopic());
        assertTrue(def.isEnabled());
        assertEquals(Map.of("url", "http://example.com/hook", "batchSize", 50), def.getProperties());
    }

    @Test
    void mapsAllTargetsIncludingDisabledOnes() {
        this.repository.save(newTarget("target-1", "http", "enabled-sink", "orders", true, Map.of()));
        this.repository.save(newTarget("target-2", "print", "disabled-sink", "audit", false, Map.of()));

        List<TargetDefinition> targets = this.registry.getTargets();

        assertEquals(2, targets.size());
        Map<String, TargetDefinition> byName = targets.stream()
                .collect(Collectors.toMap(TargetDefinition::getName, Function.identity()));
        assertTrue(byName.get("enabled-sink").isEnabled());
        TargetDefinition disabled = byName.get("disabled-sink");
        assertFalse(disabled.isEnabled());
        assertEquals("print", disabled.getType());
        assertEquals("audit", disabled.getTopic());
    }

    private static TargetEntity newTarget(String id, String type, String name, String topic,
                                          boolean enabled, Map<String, Object> properties) {
        var entity = new TargetEntity();
        entity.setId(id);
        entity.setType(type);
        entity.setName(name);
        entity.setTitle(name + " title");
        entity.setTopic(topic);
        entity.setEnabled(enabled);
        entity.setProperties(properties);
        return entity;
    }

}
