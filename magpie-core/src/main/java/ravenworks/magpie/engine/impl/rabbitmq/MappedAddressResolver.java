package ravenworks.magpie.engine.impl.rabbitmq;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AddressResolver;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


/**
 * 基于显式映射表的地址解析：把 broker metadata 返回的 advertised 地址翻译成
 * 客户端实际可达的地址（典型场景：集群跑在 Docker 中，宿主机经映射端口访问）。
 * 未命中映射的地址原样返回（透传）。
 *
 * <p>匹配基于 host+port 精确匹配（{@link Address#equals}），host 大小写敏感，
 * 配置需与 broker advertised_host 返回值一致。无状态、不可变，可并发调用。
 *
 * @author Raven
 */
@Slf4j
public class MappedAddressResolver implements AddressResolver {

    private final Map<Address, Address> mappings;

    public MappedAddressResolver(@NonNull Map<Address, Address> mappings) {
        this.mappings = Map.copyOf(mappings);
    }

    @Override
    public Address resolve(Address address) {
        Address resolved = this.mappings.getOrDefault(address, address);
        if (resolved == address) {
            log.debug("Address {} not in mappings, passing through", address);
        } else {
            log.debug("Resolved advertised address {} to {}", address, resolved);
        }
        return resolved;
    }

}
