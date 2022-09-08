package brd.asset.flink.source;

import brd.asset.entity.AssetBase;
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
 * @description: 定期读取资产基础数据
 * @author: 蒋青松
 * @create: 2022/08/11 15:14
 */
public class AssetBaseSource extends RichSourceFunction<AssetBase> {
    private boolean isRunning = true;
    private Timer timer;

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true&useSSL=false";
    private String HOST; // Leader Node host
    private String PORT;   // http port of Leader Node
    private String DB;
    private String TABLE; // 资产表名
    private String USER;
    private String PASSWD;
    private List<AssetBase> data;

    /**
     * 参数由配置文件获取传入
     *
     * @param host
     * @param port
     * @param db
     * @param user
     * @param passwd
     */
    public AssetBaseSource(String host, String port, String db, String table, String user, String passwd) {
        this.HOST = host;
        this.PORT = port;
        this.DB = db;
        this.TABLE = table;
        this.USER = user;
        this.PASSWD = passwd;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timer = new Timer(true);
    }

    @Override
    public void run(SourceContext<AssetBase> sourceContext) throws Exception {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    data = readDorisData();
                    if (data != null && data.size() > 0) {
                        //输出
                        data.stream().forEach(assetBase -> {
                            sourceContext.collect(assetBase);
                        });
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1 * 10 * 60 * 1000);
        while (isRunning) {
            if (data != null) {
                //sourceContext.collect(String.join("|", data));
            }
            Thread.sleep(10 * 1000);
        }

    }

    public List<AssetBase> readDorisData() {

        List<AssetBase> list = new ArrayList<>();

        String query = "select * from " + TABLE;
        Connection conn = null;
        PreparedStatement stmt = null;
        String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
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
                AssetBase assetBase = JSONObject.toJavaObject(jsonObject, AssetBase.class);
                list.add(assetBase);
            }
            return list;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) stmt.close();
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
        isRunning = false;
    }

}
