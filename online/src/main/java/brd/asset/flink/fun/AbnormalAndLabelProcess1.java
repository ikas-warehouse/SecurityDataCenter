package brd.asset.flink.fun;

import brd.asset.constants.AlarmItem;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.entity.EventAlarm;
import brd.asset.pojo.OpenServiceOfPort;
import brd.common.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;

/**
 * @Author: leo.j
 * @desc:
 * 1、异常资产评估
 * 2、资产基础表更新数据侧输出
 * @Date: 2022/9/14 20:40
 */
public class AbnormalAndLabelProcess1 extends ProcessFunction<AssetScanTask, EventAlarm> {
    private static Logger log = Logger.getLogger(AbnormalAndLabelProcess1.class);
    //纳管资产
    private Set<String> accessAssets = new HashSet<>();
    //资产基本表
    private Map<String, AssetBase> unique2baseMap = new HashMap<>();

    private static final String UNIQUE_CONNECT_STR = "_";

    private int openPortThreshold;
    private List<String> processBlackList;

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true&useSSL=false";
    private static final String FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private String HOST; // Leader Node host
    private String PORT;   // http port of Leader Node
    private String DB;
    private String USER;
    private String PASSWD;
    private Timer timer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                Connection conn = null;
                PreparedStatement stmt = null;
                unique2baseMap.clear();
                try {
                    DriverManager.registerDriver(new Driver());
                    String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
                    Class.forName(JDBC_DRIVER);
                    String query = "select * from asset_base";
                    conn = DriverManager.getConnection(dbUrl, USER, PASSWD);
                    stmt = conn.prepareStatement(query);
                    ResultSet rs = stmt.executeQuery();
                    JSONObject jsonObject;
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        jsonObject = new JSONObject();
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            jsonObject.put(metaData.getColumnName(i), rs.getString(i));
                        }
                        AssetBase assetBase = JSONObject.toJavaObject(jsonObject, AssetBase.class);

                        String unique = assetBase.getDevice_ip() + assetBase.getDevice_mac();

                        unique2baseMap.put(unique, assetBase);
                    }

                    log.info("更新资产数据数据成功，共" + count + "条记录！");

                } catch (Exception e) {
                    log.error("更新资产数据数据失败：" + e.getMessage());
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
        }, 100, 1 * 30 * 1 * 1000);
    }

    public AbnormalAndLabelProcess1(int openPortThreshold, String processBlack, String host, String port, String db, String user, String passwd) {
        this.HOST = host;
        this.PORT = port;
        this.DB = db;
        this.USER = user;
        this.PASSWD = passwd;
        this.openPortThreshold = openPortThreshold;
        if ("".equals(processBlack)) {
            this.processBlackList = Arrays.asList();
        } else {
            this.processBlackList = Arrays.asList(processBlack.split(","));
        }
    }


    /**
     * 获取纳管资产
     */
    public void getAccessAssets() {
        boolean isClear = false;
        for (AssetBase assetBase : unique2baseMap.values()) {
            if (!isClear) {
                accessAssets.clear();
                isClear = true;
            }
            //过滤纳管资产ip
            if ("_PERMITTED".equals(assetBase.getPermitted())) {
                String ip = assetBase.getDevice_ip();
                String mac = assetBase.getDevice_mac();
                String unique = ip + ":" + mac;
                accessAssets.add(unique);
            }
        }
    }

    @Override
    public void processElement(AssetScanTask asset, ProcessFunction<AssetScanTask, EventAlarm>.Context ctx, Collector<EventAlarm> out){
        OutputTag<AssetScanTask> insertTag = new OutputTag<AssetScanTask>("asset-base-insert") {
        };
        OutputTag<AssetScanTask> updateTag = new OutputTag<AssetScanTask>("asset-base-update") {
        };

        getAccessAssets();

        //获取所有资产ip列表
        Set<String> assets = unique2baseMap.keySet();
        String ip = asset.getDevice_ip();
        String mac = asset.getDevice_mac();
        String assetUnique = ip + UNIQUE_CONNECT_STR + mac;
        // 资产基础表新增或更新数据输出
        if (assets.contains(assetUnique)) {//ip+mac作为资产唯一性
            //更新
            ctx.output(updateTag, asset);
        } else {
            //新增
            ctx.output(insertTag, asset);
        }

        //-------- 异常资产评估 --------
        //1.异常开放端口
        List<OpenServiceOfPort> openServiceOfPorts = JSON.parseArray(asset.getOpen_service_of_port(), OpenServiceOfPort.class);
        if (openServiceOfPorts.size() > openPortThreshold) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_OPEN_PORT.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_OPEN_PORT.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);

        }
        //2.异常资产信息变更(对同一台资产os info，与上一次资产采集的信息对比)
        if (assets.contains(assetUnique)) {
            AssetBase assetBase = unique2baseMap.get(assetUnique);
            String osInfo = assetBase.getOs_info();
            if (osInfo != null && !osInfo.equals(asset.getOs_info())) {
                EventAlarm alarm = new EventAlarm();
                alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
                alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_CHANGE.getEventDesc());
                alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_CHANGE.getEventId());
                alarm.setEvent_level("中");
                alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
                alarm.setEvent_dev_ip(ip);
                alarm.setEvent_dev_mac(mac);
                out.collect(alarm);
            }
        }

        //4.发现无主资产
        if (assets.contains(assetUnique)) {
            AssetBase lastAsset = unique2baseMap.get(assetUnique);
            String attributionGroup = lastAsset.getResponsible_group();
            String responsiblePerson = lastAsset.getResponsible_person();
            if (attributionGroup == null || "".equals(attributionGroup) || responsiblePerson == null || "".equals(responsiblePerson)) {
                EventAlarm alarm = new EventAlarm();
                alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
                alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_NO_GROUP.getEventDesc());
                alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_NO_GROUP.getEventId());
                alarm.setEvent_level("中");
                alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
                alarm.setEvent_dev_ip(ip);
                alarm.setEvent_dev_mac(mac);
                out.collect(alarm);
            }
        }
        //5.发现未知资产
        if (!accessAssets.contains(assetUnique)) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_UNKNOWN.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_UNKNOWN.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);
        }
        //6.异常进程信息
        if (processBlackList.contains(ip)) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_PROESS.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_PROESS.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);
        }
    }
}
