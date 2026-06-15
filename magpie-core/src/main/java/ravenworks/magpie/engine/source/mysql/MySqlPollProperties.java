package ravenworks.magpie.engine.source.mysql;

import lombok.Data;


@Data
public class MySqlPollProperties {

    private String tableName = "magpie_outbox_message";
    private String url = "jdbc:mysql://localhost:3306/magpie?useSSL=false&characterEncoding=UTF-8";
    private String username = "mysql";
    private String password = "mysql";
    private int batchSize = 100;
    private int pollInterval = 5_000;
    private int retryDelay = 300_000;
    private String sendStrategy = "best_effort";

}
