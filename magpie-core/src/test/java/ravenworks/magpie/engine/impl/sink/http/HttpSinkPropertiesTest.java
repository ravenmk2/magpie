package ravenworks.magpie.engine.impl.sink.http;

import org.junit.jupiter.api.Test;
import ravenworks.magpie.engine.api.sink.DeliveryMode;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


class HttpSinkPropertiesTest {

    @Test
    void ofRequiresUrl() {
        assertThrows(IllegalArgumentException.class, () -> HttpSinkProperties.of(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> HttpSinkProperties.of(Map.of("url", " ")));
    }

    @Test
    void ofAppliesDefaults() {
        var props = HttpSinkProperties.of(Map.of("url", "http://localhost:8080/hook"));
        assertEquals("http://localhost:8080/hook", props.getUrl());
        assertEquals(10_000, props.getTimeout());
        assertEquals("fixed", props.getBackoff());
        assertEquals(100, props.getBatchSize());
        assertEquals(30_000, props.getCommitInterval());
        assertEquals("ORDERED", props.getDeliveryMode());
        assertEquals(1_000, props.getPersistRetryDelayMs());
        assertEquals(102, props.getRetryStatusCodes().size());
        assertTrue(props.getRetryStatusCodes().containsAll(Set.of(408, 429, 500, 599)));
        assertFalse(props.getRetryStatusCodes().contains(499));
    }

    @Test
    void ofBindsProvidedValues() {
        var props = HttpSinkProperties.of(Map.of(
                "url", "http://localhost:8080/hook",
                "timeout", 3_000,
                "batchSize", 5,
                "deliveryMode", "BEST_EFFORT"));
        assertEquals(3_000, props.getTimeout());
        assertEquals(5, props.getBatchSize());
        assertEquals("BEST_EFFORT", props.getDeliveryMode());
    }

    @Test
    void parseStatusCodesSupportsRangesAndSingles() {
        assertEquals(Set.of(200), HttpSinkProperties.parseStatusCodes("200"));
        assertEquals(Set.of(500, 501, 502), HttpSinkProperties.parseStatusCodes("500-502"));
        assertEquals(Set.of(500, 501, 502, 408), HttpSinkProperties.parseStatusCodes(" 500 - 502 , 408 "));
        assertTrue(HttpSinkProperties.parseStatusCodes("").isEmpty());
    }

    @Test
    void resolveDeliveryModeDefaultsToOrdered() {
        var props = new HttpSinkProperties();
        assertEquals(DeliveryMode.ORDERED, props.resolveDeliveryMode());
    }

    @Test
    void resolveDeliveryModeIsCaseInsensitive() {
        var props = new HttpSinkProperties();
        props.setDeliveryMode("key_ordered");
        assertEquals(DeliveryMode.KEY_ORDERED, props.resolveDeliveryMode());
        props.setDeliveryMode("best_effort");
        assertEquals(DeliveryMode.BEST_EFFORT, props.resolveDeliveryMode());
    }

    @Test
    void resolveDeliveryModeFallsBackToOrdered() {
        var props = new HttpSinkProperties();
        props.setDeliveryMode("garbage");
        assertEquals(DeliveryMode.ORDERED, props.resolveDeliveryMode());
        props.setDeliveryMode("");
        assertEquals(DeliveryMode.ORDERED, props.resolveDeliveryMode());
        props.setDeliveryMode(null);
        assertEquals(DeliveryMode.ORDERED, props.resolveDeliveryMode());
    }

}
