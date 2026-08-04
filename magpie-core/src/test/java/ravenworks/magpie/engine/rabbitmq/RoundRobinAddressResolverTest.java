package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Address;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RoundRobinAddressResolverTest {

    private static final Address IGNORED = new Address("ignored", 0);

    private static URI uri(int port) {
        return URI.create("rabbitmq-stream://guest:guest@localhost:" + port + "/%2f");
    }

    @Test
    void rotatesThroughAllUrisInOrder() {
        var resolver = new RoundRobinAddressResolver(List.of(uri(5552), uri(5553), uri(5554)));
        for (int round = 0; round < 2; round++) {
            assertEquals(5552, resolver.resolve(IGNORED).port());
            assertEquals(5553, resolver.resolve(IGNORED).port());
            assertEquals(5554, resolver.resolve(IGNORED).port());
        }
    }

    @Test
    void singleUriAlwaysResolvesSame() {
        var resolver = new RoundRobinAddressResolver(List.of(uri(5552)));
        assertEquals(5552, resolver.resolve(IGNORED).port());
        assertEquals(5552, resolver.resolve(IGNORED).port());
    }

    @Test
    void resolvesHostFromUri() {
        var resolver = new RoundRobinAddressResolver(
                List.of(URI.create("rabbitmq-stream://broker.example.com:5552")));
        assertEquals("broker.example.com", resolver.resolve(IGNORED).host());
    }

}
