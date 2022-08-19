package brd.asset.flink.task;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * @program SecurityDataCenter
 * @description: 自定义doris  source
 * @author: 蒋青松
 * @create: 2022/08/09 17:30
 */
public class CustomSourceForDoris extends RichSourceFunction<String> {
    private boolean isRunning = true;
    private Timer timer;

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s?rewriteBatchedStatements=true";
    private static final String HOST = "192.168.5.91"; // Leader Node host
    private static final int PORT = 9030;   // http port of Leader Node
    private static final String DB = "test_db";
    private static final String USER = "root";
    private static final String PASSWD = "000000";
    private List<String> data;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timer = new Timer(true);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    data = readDorisData();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1 * 1 * 10 * 1000);
        while (isRunning) {
            if(data != null){
                sourceContext.collect(String.join("|", data));
            }
            Thread.sleep(10 * 1000);
        }
    }

    public List<String> readDorisData() {

        List<String> list = new ArrayList<>();

        String query = "select * from p_01";
        Connection conn = null;
        PreparedStatement stmt = null;
        String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
        System.out.println("url: " + dbUrl);
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(dbUrl, USER, PASSWD);
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            JSONObject jsonObject;
            while (rs.next()) {
                jsonObject = new JSONObject();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    jsonObject.put(metaData.getColumnName(i), rs.getString(i));
                }
                list.add(jsonObject.toJSONString());
                System.out.println(jsonObject.toJSONString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }


}
