package brd.asset.flink.fun;

import brd.common.TimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description: 任务结束后，更新assets_task表状态
 * @author: 蒋青松
 * @create: 2022/09/07 13:27
 */
public class TaskEnd2DorisProcess extends ProcessFunction<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(TaskEnd2DorisProcess.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Properties properties;
    private String db;
    private String assetTaskTB;

    public TaskEnd2DorisProcess(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(properties.getProperty("url"),
                properties.getProperty("username"),
                properties.getProperty("password"));
        db = properties.getProperty("db");
        assetTaskTB = properties.getProperty("table");
    }

    @Override
    public void processElement(String taskId, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        String finishTime = TimeUtils.getNow("yyyy-MM-dd HH:mm:ss");
        String sql = "update " + db + "." + assetTaskTB + " set finish_time='" + finishTime + "' , task_state=2 where task_id=" + taskId;
        System.out.println("sql:" + sql);
        try {
            ps = connection.prepareStatement(sql);
            ps.execute();
            logger.info("task_id: " + taskId + "执行完成！");
            System.out.println("task_id: " + taskId + "执行完成！");
        } catch (Exception e) {
            logger.error("更新任务结束状态异常，msg=" + e.getMessage());
            System.out.println("更新任务结束状态异常，msg=" + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (ps != null) ps.close();
        } catch (SQLException se2) {
            logger.error(se2.getMessage());
        }
        try {
            if (connection != null) connection.close();
        } catch (SQLException se) {
            logger.error(se.getMessage());
        }
    }
}
