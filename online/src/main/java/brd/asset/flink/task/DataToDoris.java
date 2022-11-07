package brd.asset.flink.task;

import brd.asset.flink.fun.JsonFilterFunction;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.asset.flink.sink.KafkaDorisSink;
import brd.common.KafkaUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @program DataToDoris
 * @description:
 * 1. 泛化表表入库
 * 2. cve漏洞表入库
 * 3. cnvd漏洞表入库
 * 4. 威胁情报表入库
 * 5. 单独处理漏扫表入库., 更改漏洞等级vulnerability_level.
 * @author: 张世钰
 * @create: 2022/10/18 9:47
 */
public class DataToDoris {
    public static void main(String[] args) throws Exception {
        //获取配置数据
        //String propPath = "D:\\DevelopData\\IDEAData\\securitydatacenter\\online\\src\\main\\resources\\asset_process.properties";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //------------------------------------获取配置数据 begin--------------------------------------
        //kafka properties
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String brokers = paramFromProps.get("consumer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");
        //kafka topic
        String formatTopic = paramFromProps.get("formatTopic");
        String cveTopic = paramFromProps.get("cveTopic");
        String cnvdTopic = paramFromProps.get("cnvdTopic");
        String tjTopic = paramFromProps.get("tjTopic");
        String scanTopic = paramFromProps.get("scanTopic");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        //doris tables
        String formatTb = paramFromProps.get("dorisTable.eventFormat");
        String formatField = paramFromProps.get("formatField");
        String formatKey = paramFromProps.get("formatKey");
        String cnvdTb = paramFromProps.get("dorisTable.cnvdVulnerability");
        String cnvdField = paramFromProps.get("cnvdField");
        String cnvdKey = paramFromProps.get("cnvdKey");
        String cveTb = paramFromProps.get("dorisTable.cveVulnerability");
        String cveField = paramFromProps.get("cveField");
        String cveKey = paramFromProps.get("cveKey");
        String tjTb = paramFromProps.get("dorisTable.tjThreat");
        String tjField = paramFromProps.get("tjField");
        String tjKey = paramFromProps.get("tjKey");
        String scanTb = paramFromProps.get("dorisTable.scanVulnerability");
        String scanField = paramFromProps.get("scanField");
        String scanKey = paramFromProps.get("scanKey");
        //parallelism
        Integer commonParallelism = paramFromProps.getInt("import.commonParallelism");
        Integer kafkaParallelism = paramFromProps.getInt("import.kafkaParallelism");
        Integer dorisSinkParallelism = paramFromProps.getInt("import.dorisSinkParallelism");
        //------------------------------------获取配置数据 end--------------------------------------


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(commonParallelism);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //------------------------------------------------------------------------------------------
        //泛化表入库
        Properties formatPro = new Properties();
        formatPro.setProperty("brokers", brokers);
        formatPro.setProperty("topic", formatTopic);
        formatPro.setProperty("groupId", groupId);
        formatPro.setProperty("host", dorisHost);
        formatPro.setProperty("port", dorisPort);
        formatPro.setProperty("db", dorisDB);
        formatPro.setProperty("username", dorisUser);
        formatPro.setProperty("password", dorisPw);
        formatPro.setProperty("table", formatTb);
        formatPro.setProperty("fieldString", formatField);
        formatPro.setProperty("keyString", formatKey);
        formatPro.setProperty("labelPrefix", "event-format-" + System.currentTimeMillis());
        KafkaDorisSink formatSink = new KafkaDorisSink(env, formatPro, kafkaParallelism, dorisSinkParallelism);
        formatSink.sink();
        
        //------------------------------------------------------------------------------------------
        //cnvd漏洞表入库
        Properties cnvdPro = new Properties();
        cnvdPro.setProperty("brokers", brokers);
        cnvdPro.setProperty("topic", cnvdTopic);
        cnvdPro.setProperty("groupId", groupId);
        cnvdPro.setProperty("host", dorisHost);
        cnvdPro.setProperty("port", dorisPort);
        cnvdPro.setProperty("db", dorisDB);
        cnvdPro.setProperty("username", dorisUser);
        cnvdPro.setProperty("password", dorisPw);
        cnvdPro.setProperty("table", cnvdTb);
        cnvdPro.setProperty("fieldString", cnvdField);
        cnvdPro.setProperty("keyString", cnvdKey);
        cnvdPro.setProperty("labelPrefix", "cnvd-vulnerability-" + System.currentTimeMillis());
        KafkaDorisSink cnvdSink = new KafkaDorisSink(env, cnvdPro, kafkaParallelism, dorisSinkParallelism);
        cnvdSink.sink();

        //------------------------------------------------------------------------------------------
        //cve漏洞表入库
        Properties cvePro = new Properties();
        cvePro.setProperty("brokers", brokers);
        cvePro.setProperty("topic", cveTopic);
        cvePro.setProperty("groupId", groupId);
        cvePro.setProperty("host", dorisHost);
        cvePro.setProperty("port", dorisPort);
        cvePro.setProperty("db", dorisDB);
        cvePro.setProperty("username", dorisUser);
        cvePro.setProperty("password", dorisPw);
        cvePro.setProperty("table", cveTb);
        cvePro.setProperty("fieldString", cveField);
        cvePro.setProperty("keyString", cveKey);
        cvePro.setProperty("labelPrefix", "cve-vulnerability-" + System.currentTimeMillis());
        KafkaDorisSink cveSink = new KafkaDorisSink(env, cvePro, kafkaParallelism, dorisSinkParallelism);
        cveSink.sink();

        //------------------------------------------------------------------------------------------
        //天际情报表入库
        Properties tjPro = new Properties();
        tjPro.setProperty("brokers", brokers);
        tjPro.setProperty("topic", tjTopic);
        tjPro.setProperty("groupId", groupId);
        tjPro.setProperty("host", dorisHost);
        tjPro.setProperty("port", dorisPort);
        tjPro.setProperty("db", dorisDB);
        tjPro.setProperty("username", dorisUser);
        tjPro.setProperty("password", dorisPw);
        tjPro.setProperty("table", tjTb);
        tjPro.setProperty("fieldString", tjField);
        tjPro.setProperty("keyString", tjKey);
        tjPro.setProperty("labelPrefix", "tj-threat-" + System.currentTimeMillis());
        KafkaDorisSink tjSink = new KafkaDorisSink(env, tjPro, kafkaParallelism, dorisSinkParallelism);
        tjSink.sink();

        //-----------------------------------------------------------------------------------
        //获取漏洞信息 vulner-kafka-source
        KafkaSource<String> scanKafkaSource = KafkaUtils.getKafkaSource(brokers, scanTopic, groupId);
        DataStreamSource<String> scanSource = env.fromSource(scanKafkaSource, WatermarkStrategy.noWatermarks(), "scan-kafka-source").setParallelism(kafkaParallelism);

        //过滤不需要的字段
        SingleOutputStreamOperator<JSONObject> scanDS = scanSource.process(new JsonFilterFunction(scanField, scanKey));

        //更改漏洞等级等级 vulnerability_level
        SingleOutputStreamOperator<JSONObject> scanJsonDS = scanDS.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject result) throws Exception {
                int vulnerability_level = Integer.parseInt(result.getString("vulnerability_level"));
                if (vulnerability_level < 1) {
                    result.put("vulnerability_level", "1");
                } else if (vulnerability_level > 3) {
                    result.put("vulnerability_level", "3");
                }
                return result;
            }
        });

        //写入scan_vulnerability表
        Properties scanPro = new Properties();
        scanPro.setProperty("host", dorisHost);
        scanPro.setProperty("port", dorisPort);
        scanPro.setProperty("username", dorisUser);
        scanPro.setProperty("password", dorisPw);
        scanPro.setProperty("db", dorisDB);
        scanPro.setProperty("table", scanTb);
        scanPro.setProperty("labelPrefix", "scan-vulnerability-" + System.currentTimeMillis());
        AssetDataCommonSink scanSink = new AssetDataCommonSink(env, scanJsonDS, scanPro, dorisSinkParallelism);
        scanSink.sink();

        env.execute("data to doris");

    }
}
