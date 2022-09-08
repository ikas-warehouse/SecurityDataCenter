package brd.asset.flink.fun;

import brd.asset.entity.AssetScanTask;
import brd.asset.pojo.AssetScanOrigin;
import brd.asset.pojo.DataBaseInfoScan;
import brd.asset.pojo.MessageOrientedMiddlewareScan;
import brd.asset.pojo.OpenServiceOfPort;
import brd.common.TimeUtils;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.Driver;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * @Author: leo.j
 * @desc: 回填标签信息
 * @Date: 2022/3/25 1:22 下午
 */
public class LabeledMap extends RichMapFunction<AssetScanTask, AssetScanTask> {
    private static Logger LOG = Logger.getLogger(LabeledMap.class);

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true&useSSL=false";
    private static final String FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private String HOST; // Leader Node host
    private String PORT;   // http port of Leader Node
    private String DB;
    private String USER;
    private String PASSWD;

    private Map<String, Tuple3<Integer, String, String>> labelMap;
    private Timer timer;

    public LabeledMap(String host, String port, String db, String user, String passwd) {
        this.HOST = host;
        this.PORT = port;
        this.DB = db;
        this.USER = user;
        this.PASSWD = passwd;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        labelMap = new HashMap<>();
        //开启一个定时任务，定期更新配置数据
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Connection conn = null;
                PreparedStatement stmt = null;
                try {
                    DriverManager.registerDriver(new Driver());
                    String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
                    Class.forName(JDBC_DRIVER);
                    String query = "select l.id as id,l.label_1 as label1,l.label_2 as label2,k.keyword as keyword from label l join label_key k on l.id = k.label_id";
                    conn = DriverManager.getConnection(dbUrl, USER, PASSWD);
                    stmt = conn.prepareStatement(query);
                    ResultSet rs = stmt.executeQuery();

                    int count = 0;
                    while (rs.next()) {
                        int id = rs.getInt("id");
                        String label1 = rs.getString("label1");
                        String label2 = rs.getString("label2");
                        String keyword = rs.getString("keyword");
                        //标签数据是累加，不做清空
                        labelMap.put(keyword, Tuple3.of(id, label1, label2));
                        count++;
                    }

                    LOG.info("更新标签数据成功，共" + count + "条记录！");

                } catch (Exception e) {
                    LOG.error("更新标签数据失败：" + e.getMessage());
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
            }
        }, 100, 1 * 30 * 60 * 1000);
    }

    @Override
    public AssetScanTask map(AssetScanTask scan) throws Exception {
        if (labelMap != null) {

            //时间格式校对,满足doris的datetime格式
            String scanTime = scan.getScan_time();
            boolean validDate = TimeUtils.validDate(scanTime, FORMAT_PATTERN);
            if(!validDate){
                return null;
            }

            String taskID = scan.getTask_id();
            String messageOrientedMiddlewares = scan.getMessage_oriented_middleware();
            String dataBaseInfos = scan.getData_base_info();
            String openServiceOfPorts = scan.getOpen_service_of_port();



            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(messageOrientedMiddlewares)
                    .append(",")
                    .append(dataBaseInfos)
                    .append(",")
                    .append(openServiceOfPorts)
                    .append(",")
                    .append(scan.getOs_info())
                    .append(",")
                    .append(scan.getSystem_fingerprint_info())
                    .append(",")
                    .append(scan.getDevice_type());

            String labelInputInfo = stringBuilder.toString();

            Set<Tuple3<Integer, String, String>> matchedLabels = matchLabel(labelInputInfo);

            if (matchedLabels.size() == 0) {
                System.out.println("scan1: " + scan);
                return scan;
            } else {
                String separator = ",";
                List<String> labelIds = new ArrayList<>();
                List<String> type1List = new ArrayList<>();
                List<String> type2List = new ArrayList<>();
                for (Tuple3<Integer, String, String> label : matchedLabels) {
                    labelIds.add(String.valueOf(label.f0));
                    type1List.add(label.f1);
                    type2List.add(label.f2);
                }
                scan.setLabel_id(StringUtils.join(labelIds, separator));
                System.out.println("scan2: " + scan);

                return scan;
            }
        }
        return null;
    }

    /**
     * @param input 指纹信息
     * @return labels
     */
    public Set<Tuple3<Integer, String, String>> matchLabel(String input) {
        Set<Tuple3<Integer, String, String>> labels = new HashSet<Tuple3<Integer, String, String>>();
        Set<String> keywords = labelMap.keySet();
        for (String keyword : keywords) {
            if (input.toLowerCase().contains(keyword.toLowerCase())) {
                labels.add(labelMap.get(keyword));
            }
        }
        return labels;
    }
}
