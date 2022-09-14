package brd.asset.flink.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description:  通用的DataStream sink doris
 * 需要注意几点
 * 1、数据的字段和doris表字段要对应上，否则不能正常sink！！！datetime可以用string对应
 * 2、labelPrefix需要配置不同，短期联调需要更改该值
 * @author: 蒋青松
 * @create: 2022/09/06 18:19
 */
public class AssetDataCommonSink<T> {
    private StreamTableEnvironment tEnv;
    private DataStream<T> data;
    private String db;
    private String tb;
    private String host;
    private String port;
    private String user;
    private String pw;
    private String labelPrefix;

    public AssetDataCommonSink(StreamExecutionEnvironment env, DataStream<T> data, Properties properties) {
        this.data = data;
        this.db = properties.getProperty("db");
        this.tb = properties.getProperty("table");
        this.host = properties.getProperty("host");
        this.port = properties.getProperty("port");
        this.user = properties.getProperty("username");
        this.pw = properties.getProperty("password");
        this.labelPrefix = properties.getProperty("labelPrefix");
        this.tEnv = StreamTableEnvironment.create(env);
    }

    public void sink() {

        //方法1：flink sql -- 不稳定，有问题待复查

        /*Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled", "true");
        configuration.setString("table.exec.mini-batch.allow-latency", "5s");
        configuration.setString("table.exec.mini-batch.size", "50");
        configuration.setString("table.dml-sync", "false");
//Caused by: java.lang.IllegalArgumentException: Time interval unit label 'l' does not match any of the recognized units: DAYS: (d | day | days), HOURS: (h | hour | hours), MINUTES: (min | minute | minutes), SECONDS: (s | sec | secs | second | seconds), MILLISECONDS: (ms | milli | millis | millisecond | milliseconds), MICROSECONDS: (µs | micro | micros | microsecond | microseconds), NANOSECONDS: (ns | nano | nanos | nanosecond | nanoseconds)
        //set checkpointing.interval = 10 second via configuration
        configuration.setString("execution.checkpointing.interval", "10s");
        String sql =
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
                        "  'doris.batch.size' = '1',\n" +
                        "  'username' = '" + user + "',\n" +
                        "  'password' = '" + pw + "'\n" +
                        ")";
        tEnv.executeSql(sql);
        Table table = tEnv.fromDataStream(scan);
        tEnv.executeSql("insert into asset_task_origin select * from " + table);*/

        //方法2：DataStream
        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        pro.setProperty("line_delimiter", "\n");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(host + ":" + port)
                .setTableIdentifier(db + "." + tb)
                .setUsername(user)
                .setPassword(pw);
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setStreamLoadProp(pro)
                .setLabelPrefix(labelPrefix);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        SingleOutputStreamOperator<String> jsonDS = data.map(x -> JSONObject.toJSONString(x));

        jsonDS.sinkTo(builder.build());


    }
}
