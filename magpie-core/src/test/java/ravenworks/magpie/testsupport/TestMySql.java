package ravenworks.magpie.testsupport;

import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 * 集成测试共享的 MySQL 容器（Testcontainers singleton）：整个测试 JVM 只起一个实例。
 *
 * <p>容器自带的 test 用户只有默认 test 库的权限（不能 CREATE DATABASE），
 * 因此所有 IT 共享这一个库，用 {@link #reset()} 做隔离：建表一次（schema.sql 幂等），
 * 然后 TRUNCATE 全部 magpie_* 表，给每个测试类干净起点。IT 串行执行（failsafe 默认），
 * 共享库不构成冲突。
 *
 * <p>建表脚本直接执行仓库里的 docs/database/schema.sql——IT 的目的之一就是验证
 * 这份脚本能在真实 MySQL 上跑通，因此不复制副本。路径相对模块工作目录解析
 * （surefire/failsafe 以模块 basedir 为工作目录）。
 */
public final class TestMySql {

    private static final Path SCHEMA_SQL = Path.of("..", "docs", "database", "schema.sql");

    private static final MySQLContainer<?> CONTAINER =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    private static boolean schemaInitialized;

    static {
        CONTAINER.start();
    }

    private TestMySql() {
    }

    /**
     * 返回指向共享测试库的 DataSource。首次调用按 docs/database/schema.sql 建表，
     * 随后 TRUNCATE 所有 magpie_* 表。每个测试类在 @BeforeAll 调用一次做隔离。
     */
    public static synchronized DataSource reset() {
        DataSource dataSource = dataSource();
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        if (!schemaInitialized) {
            try (Connection connection = dataSource.getConnection()) {
                ScriptUtils.executeSqlScript(connection, new FileSystemResource(SCHEMA_SQL));
            } catch (SQLException e) {
                throw new IllegalStateException("Failed to apply " + SCHEMA_SQL, e);
            }
            schemaInitialized = true;
        }
        List<String> tables = jdbc.queryForList(
                "SELECT table_name FROM information_schema.tables"
                        + " WHERE table_schema = DATABASE() AND table_name LIKE 'magpie\\_%'",
                String.class);
        jdbc.execute("SET FOREIGN_KEY_CHECKS = 0");
        tables.forEach(table -> jdbc.execute("TRUNCATE TABLE `" + table + "`"));
        jdbc.execute("SET FOREIGN_KEY_CHECKS = 1");
        return dataSource;
    }

    private static DataSource dataSource() {
        var dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(CONTAINER.getJdbcUrl() + "?useSSL=false&allowPublicKeyRetrieval=true");
        dataSource.setUsername(CONTAINER.getUsername());
        dataSource.setPassword(CONTAINER.getPassword());
        return dataSource;
    }

}
