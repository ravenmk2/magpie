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
        this.uris = List.copyOf(uris);
    }

    @Override
    public Address resolve(Address address) {
        int idx = counter.getAndUpdate(i -> (i + 1) % uris.size());
        URI uri = uris.get(idx);
        return new Address(uri.getHost(), uri.getPort());
    }

}
