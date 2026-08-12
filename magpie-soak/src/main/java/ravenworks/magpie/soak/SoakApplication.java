package ravenworks.magpie.soak;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * soak 工具入口：SPRING_PROFILES_ACTIVE=loadgen|verifier 决定角色
 */
@SpringBootApplication
public class SoakApplication {

    public static void main(String[] args) {
        SpringApplication.run(SoakApplication.class, args);
    }

}
