package ravenworks.magpie.testkit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * 系统测试工装入口：SPRING_PROFILES_ACTIVE=loadgen|verifier 决定角色
 */
@SpringBootApplication
public class TestkitApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestkitApplication.class, args);
    }

}
