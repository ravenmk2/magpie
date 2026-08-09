package ravenworks.magpie.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;


@ConfigurationPropertiesScan
@SpringBootApplication(scanBasePackages = "ravenworks.magpie")
@EntityScan(basePackages = "ravenworks.magpie.domain.entity")
@EnableJpaRepositories(basePackages = "ravenworks.magpie.domain.repository")
public class MagpieApplication {

    static void main(String[] args) {
        SpringApplication.run(MagpieApplication.class, args);
    }

}
