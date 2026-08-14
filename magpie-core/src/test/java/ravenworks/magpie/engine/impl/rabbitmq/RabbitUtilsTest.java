package ravenworks.magpie.engine.impl.rabbitmq;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class RabbitUtilsTest {

    @Test
    void parsesHostAndPort() {
        var address = RabbitUtils.parseAddress("broker-1.internal:5552");
        assertEquals("broker-1.internal", address.host());
        assertEquals(5552, address.port());
    }

    @Test
    void splitsAtLastColon() {
        // host 部分允许含冒号（如带方括号的 IPv6 字面量）
        var address = RabbitUtils.parseAddress("[::1]:5552");
        assertEquals("[::1]", address.host());
        assertEquals(5552, address.port());
    }

    @Test
    void rejectsMissingPort() {
        assertThrows(IllegalArgumentException.class, () -> RabbitUtils.parseAddress("broker-1"));
        assertThrows(IllegalArgumentException.class, () -> RabbitUtils.parseAddress("broker-1:"));
    }

    @Test
    void rejectsNonNumericPort() {
        assertThrows(IllegalArgumentException.class, () -> RabbitUtils.parseAddress("broker-1:abc"));
    }

}
