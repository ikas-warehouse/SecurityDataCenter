package brd.asset.flink.sink;

import brd.asset.entity.AssetScanTask;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description: 资产原始数据入库
 * @author: 蒋青松
 * @create: 2022/09/06 18:19
 */
public class AssetScanOriginSink {
    private StreamTableEnvironment tEnv;
    private SingleOutputStreamOperator<AssetScanTask> scan;
    private String db;
    private String tb;
    private String host;
    private String port;
    private String batchSize;
    private String user;
    private String pw;

    public AssetScanOriginSink(StreamExecutionEnvironment env, SingleOutputStreamOperator<AssetScanTask> scanDS, Properties properties) {
        this.scan = scanDS;
        this.db = properties.getProperty("db");
        this.tb = properties.getProperty("table");
        this.host = properties.getProperty("host");
        this.port = properties.getProperty("port");
        this.user = properties.getProperty("user");
        this.pw = properties.getProperty("pw");
        this.batchSize = properties.getProperty("batchSize");
        this.tEnv = StreamTableEnvironment.create(env);
    }

    public void sink() {
        tEnv.executeSql(
                "CREATE TABLE asset_task_origin (\n" +
                        "asset_id string ,\n" +
                        "task_id string,\n" +
                        "scan_time string,\n" +
                        "device_name string,\n" +
                        "device_ip string,\n" +
                        "device_ip_ownership string,\n" +
                        "device_mac string,\n" +
                        "device_type string,\n" +
                        "data_base_info string,\n" +
                        "kernel_version string,\n" +
                        "message_oriented_middleware string,\n" +
                        "open_service_of_port string,\n" +
                        "os_info string,\n" +
                        "patch_properties string,\n" +
                        "program_info string,\n" +
                        "system_fingerprint_info string,\n" +
                        "label_id string,\n" +
                        "label_type1 string,\n" +
                        "label_type2 string\n" +
                        ") \n" +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '" + host + ":" + port + "',\n" +
                        "  'table.identifier' = '" + db + "." + tb + "',\n" +
                        "  'sink.batch.size' = '" + batchSize + "',\n" +
                        "  'sink.batch.interval' = 1'\n" +
                        "  'username' = '" + user + "',\n" +
                        "  'password' = '" + pw + "'\n" +
                        ")");
        Table table = tEnv.fromDataStream(scan);
        tEnv.executeSql("insert into asset_task_origin select * from " + table);
    }
}
