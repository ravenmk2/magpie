package ravenworks.magpie.engine.impl.source.mysql;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * 测试用 H2 触发器：挂在 outbox 表上让 DELETE 必失败，模拟 deleteBatch 故障。
 * H2 反射加载触发器类，必须是 public 顶层类。
 */
public class SabotageDeleteTrigger implements org.h2.api.Trigger {

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        throw new SQLException("sabotaged delete");
    }

}
