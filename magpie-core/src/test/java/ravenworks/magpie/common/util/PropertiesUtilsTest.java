package ravenworks.magpie.common.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class PropertiesUtilsTest {

    static class Bean {

        private String name = "default";
        private int count = 1;

        public String getName() {
            return this.name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getCount() {
            return this.count;
        }

        public void setCount(int count) {
            this.count = count;
        }

    }

    @Test
    void nullSourceIsNoop() {
        var bean = new Bean();
        PropertiesUtils.bind(bean, null);
        assertEquals("default", bean.getName());
        assertEquals(1, bean.getCount());
    }

    @Test
    void emptySourceIsNoop() {
        var bean = new Bean();
        PropertiesUtils.bind(bean, Map.of());
        assertEquals("default", bean.getName());
        assertEquals(1, bean.getCount());
    }

    @Test
    void bindsProvidedFieldsAndKeepsDefaults() {
        var bean = new Bean();
        PropertiesUtils.bind(bean, Map.of("name", "bound"));
        assertEquals("bound", bean.getName());
        assertEquals(1, bean.getCount());
    }

    @Test
    void bindsNumericValues() {
        var bean = new Bean();
        PropertiesUtils.bind(bean, Map.of("count", 5));
        assertEquals(5, bean.getCount());
    }

    @Test
    void unknownPropertiesAreIgnored() {
        var bean = new Bean();
        assertDoesNotThrow(() -> PropertiesUtils.bind(bean, Map.of("unknown", "v")));
        assertEquals("default", bean.getName());
        assertEquals(1, bean.getCount());
    }

    @Test
    void repeatedBindsMerge() {
        var bean = new Bean();
        PropertiesUtils.bind(bean, Map.of("name", "bound"));
        PropertiesUtils.bind(bean, Map.of("count", 5));
        assertEquals("bound", bean.getName());
        assertEquals(5, bean.getCount());
    }

    @Test
    void numericStringIsCoercedToInt() {
        // Jackson 默认的 String→int 强转："5" 可绑定进 int 字段
        var bean = new Bean();
        PropertiesUtils.bind(bean, Map.of("count", "5"));
        assertEquals(5, bean.getCount());
    }

    @Test
    void nonNumericStringFailsToBindIntoIntField() {
        // 类型不匹配无法强转：bind 包装为 RuntimeException 抛出
        var bean = new Bean();
        var e = assertThrows(RuntimeException.class,
                () -> PropertiesUtils.bind(bean, Map.of("count", "abc")));
        assertTrue(e.getMessage().startsWith("Failed to bind properties to"),
                "unexpected message: " + e.getMessage());
    }

}
