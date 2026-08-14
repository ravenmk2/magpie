package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Address;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;


class MappedAddressResolverTest {

    private static final Address ADVERTISED = new Address("broker-1.internal", 5552);
    private static final Address REACHABLE = new Address("192.168.1.10", 15552);

    @Test
    void mappedAddressTranslated() {
        var resolver = new MappedAddressResolver(Map.of(ADVERTISED, REACHABLE));
        assertEquals(REACHABLE, resolver.resolve(ADVERTISED));
    }

    @Test
    void unmappedAddressPassedThrough() {
        var resolver = new MappedAddressResolver(Map.of(ADVERTISED, REACHABLE));
        Address other = new Address("broker-2.internal", 5552);
        assertSame(other, resolver.resolve(other));
    }

    @Test
    void emptyMappingsPassThrough() {
        var resolver = new MappedAddressResolver(Map.of());
        assertSame(ADVERTISED, resolver.resolve(ADVERTISED));
    }

    @Test
    void matchingRequiresHostAndPort() {
        // 同 host 不同 port 视为不同地址，不命中映射
        var resolver = new MappedAddressResolver(Map.of(ADVERTISED, REACHABLE));
        Address differentPort = new Address("broker-1.internal", 5553);
        assertSame(differentPort, resolver.resolve(differentPort));
    }

}
