package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AddressResolver;
import lombok.NonNull;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Raven
 */
public class RoundRobinAddressResolver implements AddressResolver {

    private final List<URI> uris;
    private final AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinAddressResolver(@NonNull List<URI> uris) {
        if (uris.isEmpty()) {
            throw new IllegalArgumentException("uris must not be empty");
        }
        this.uris = List.copyOf(uris);
    }

    @Override
    public Address resolve(Address address) {
        int idx = this.counter.getAndUpdate(i -> (i + 1) % this.uris.size());
        URI uri = this.uris.get(idx);
        return new Address(uri.getHost(), uri.getPort());
    }

}
