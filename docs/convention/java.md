# Java 编码规范

## 实例成员访问必须加 `this.` 前缀

访问当前对象的实例字段和实例方法时，一律显式加 `this.` 前缀，包括构造器、普通方法、lambda 和方法引用（`this::method`）中的访问：

```java
this.counter++;
this.dispatch(record);
this.workLoop.submit(this::poll);
```

以下情况**不**加 `this.`：

- static 字段（含常量、Lombok 生成的 `log`）和 static 方法；
- 被局部变量、方法参数、lambda 参数等同名标识符遮蔽的场景——此时裸名指的是局部变量而非字段，加 `this.` 会改变语义；
- static 方法、static 初始化块、static 嵌套类内部（没有 `this` 上下文）；
- `this(...)` / `super(...)` 构造器委托和 `super.xxx` 访问；
- 非 static 内部类/匿名类中访问外层类的实例成员——应写成 `OuterClassName.this.member`；
- 已通过其它对象限定的访问（如 `record.getKey()`）。
