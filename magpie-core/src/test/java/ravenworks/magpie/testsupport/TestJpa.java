package ravenworks.magpie.testsupport;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import javax.sql.DataSource;
import java.util.Map;


/**
 * 集成测试用的最小 Spring 上下文：真实 DataSource + Hibernate + Spring Data JPA，
 * 不依赖 Spring Boot。与生产装配（magpie-server 的 spring-boot-starter-data-jpa）
 * 使用同一套 repository 接口和实体映射，差别仅在装配方式。
 *
 * <p>hbm2ddl 固定为 none——表结构一律来自 docs/database/schema.sql
 * （见 {@link TestMySql#createDatabase}），DDL 漂移在 IT 中直接暴露。
 */
public final class TestJpa {

    private TestJpa() {
    }

    public static AnnotationConfigApplicationContext create(DataSource dataSource) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(DataSource.class, () -> dataSource);
        context.register(JpaConfig.class);
        context.refresh();
        return context;
    }

    @Configuration
    @EnableJpaRepositories(basePackages = "ravenworks.magpie.domain.repository")
    static class JpaConfig {

        @Bean
        LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
            var emf = new LocalContainerEntityManagerFactoryBean();
            emf.setDataSource(dataSource);
            emf.setPackagesToScan("ravenworks.magpie.domain.entity");
            emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
            emf.setJpaPropertyMap(Map.of("hibernate.hbm2ddl.auto", "none"));
            return emf;
        }

        @Bean
        JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
            return new JpaTransactionManager(entityManagerFactory);
        }

    }

}
