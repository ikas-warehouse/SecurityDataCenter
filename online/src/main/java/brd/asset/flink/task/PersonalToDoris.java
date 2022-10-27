package brd.asset.flink.task;

import brd.asset.flink.fun.JsonFilterFunction;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.asset.flink.sink.JdbcDorisSink;
import brd.common.KafkaUtil;
import brd.common.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @program PersonalToDoris
 * @description: 1.单独处理漏扫表, 更改漏洞等级vulnerability_level.
 * 2.监听kafka修改告警表 处理字段 & 处理时间字段
 * 3.监听kafka修改告警表 规则字段
 * @author: 张世钰
 * @create: 2022/10/12 10:37
 */
public class PersonalToDoris {
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
        String vulnerTopic = paramFromProps.get("vulnerTopic");
        String handleTopic = paramFromProps.get("handleTopic");
        String ruleTopic = paramFromProps.get("ruleTopic");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        //jdbc properties
        String jdbc_port = paramFromProps.get("dorisPort");
        String jdbc_driver = paramFromProps.get("jdbc_driver");
        String db_url_pattern = paramFromProps.get("db_url_pattern");
        //parallelism
        Integer commonParallelism = paramFromProps.getInt("import.commonParallelism");
        Integer kafkaParallelism = paramFromProps.getInt("import.kafkaParallelism");
        Integer dorisSinkParallelism = paramFromProps.getInt("import.dorisSinkParallelism");
        Integer updateCommonParallelism = paramFromProps.getInt("update.commonParallelism");
        Integer updateKafkaParallelism = paramFromProps.getInt("update.kafkaParallelism");
        Integer updateJdbcSinkParallelism = paramFromProps.getInt("update.jdbcSinkParallelism");
        
        //------------------------------------获取配置数据 end--------------------------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(commonParallelism);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //----------------------------------------------------------------------------------------

        //获取漏洞信息 vulner-kafka-source
        KafkaSource<String> vulnerKafkaSource = KafkaUtil.getKafkaSource(brokers, vulnerTopic, groupId);
        DataStreamSource<String> vulnerSource = env.fromSource(vulnerKafkaSource, WatermarkStrategy.noWatermarks(), "vulner-kafka-source").setParallelism(kafkaParallelism);

        //漏扫表字段
        String vulnerStrings = "scan_ip,vulnerability_id,task_id,record_time,state,vulnerability_name,description,cve_id,cvnd_id,cvnnd_id,vulnerability_type,solution,vulnerability_level,product,repair_status";
        //过滤不需要字段
        SingleOutputStreamOperator<JSONObject> vulnerDS = vulnerSource.process(new JsonFilterFunction(vulnerStrings));

        //更改漏洞等级等级 vulnerability_level
        SingleOutputStreamOperator<JSONObject> vulnerJsonDS = vulnerDS.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject result, Collector<JSONObject> collector) throws Exception {
                if (result.get("scan_ip") != null && result.get("vulnerability_level") != null) {
                    int vulnerability_level = Integer.parseInt(result.getString("vulnerability_level"));
                    if (vulnerability_level < 1) {
                        result.put("vulnerability_level", "1");
                    } else if (vulnerability_level > 3) {
                        result.put("vulnerability_level", "3");
                    }
                    collector.collect(result);
                }

            }
        });

        //写入scan_vulnerability表
        Properties vulnerPro = new Properties();
        vulnerPro.setProperty("host", dorisHost);
        vulnerPro.setProperty("port", dorisPort);
        vulnerPro.setProperty("username", dorisUser);
        vulnerPro.setProperty("password", dorisPw);
        vulnerPro.setProperty("db", dorisDB);
        vulnerPro.setProperty("table", "scan_vulnerability");
        vulnerPro.setProperty("labelPrefix", "scan-vulnerability-" + System.currentTimeMillis());
        AssetDataCommonSink vulnerSink = new AssetDataCommonSink(env, vulnerJsonDS, vulnerPro, dorisSinkParallelism);
        vulnerSink.sink();
        //----------------------------------------------------------------------------------------

        //获取告警处理信息 alarm_handle-kafka-source
        KafkaSource<String> alarm_handleKafkaSource = KafkaUtil.getKafkaSource(brokers, handleTopic, groupId);
        DataStreamSource<String> alarm_handleSource = env.fromSource(alarm_handleKafkaSource, WatermarkStrategy.noWatermarks(), "alarm_handle-kafka-source").setParallelism(updateKafkaParallelism);

        //转型JSONObject 添加:处理字段 时间字段
        SingleOutputStreamOperator<JSONObject> alarm_handleJsonDS = alarm_handleSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                if (StringUtils.isjson(s)) {
                    JSONObject jsonObject = JSON.parseObject(s);
                    if ((!jsonObject.isEmpty()) && jsonObject.get("event_id") != null) {
                        jsonObject.put("handle", "1");

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        jsonObject.put("handle_time", dateFormat.format(System.currentTimeMillis()));
                        collector.collect(jsonObject);
                    }
                }
            }
        }).setParallelism(updateCommonParallelism);

        //更新告警表
        Properties handlePro = new Properties();
        handlePro.setProperty("jdbc_driver", jdbc_driver);
        handlePro.setProperty("db_url_pattern", db_url_pattern);
        handlePro.setProperty("host", dorisHost);
        handlePro.setProperty("port", jdbc_port);
        handlePro.setProperty("db", dorisDB);
        handlePro.setProperty("user", dorisUser);
        handlePro.setProperty("passwd", dorisPw);

        String handleSql = "update" + " sdc.event_alarm" + " set handle=?, handle_time=? where event_id=?";
        String handleStrings = "handle,handle_time,event_id"; //按占位符?顺序
        alarm_handleJsonDS.addSink(new JdbcDorisSink<>(handleSql, handleStrings, handlePro)).setParallelism(updateJdbcSinkParallelism);
        //-------------------------------------------------------------------------------

        //读取告警规则信息 alarm_rule-kafka-source
        KafkaSource<String> alarm_ruleKafkaSource = KafkaUtil.getKafkaSource(brokers, ruleTopic, groupId);
        DataStreamSource<String> alarm_ruleSource = env.fromSource(alarm_ruleKafkaSource, WatermarkStrategy.noWatermarks(), "alarm_rule-kafka-source").setParallelism(updateKafkaParallelism);

        //转型成JSONObject 添加 处理字段 处理事件字段
        SingleOutputStreamOperator<JSONObject> alarm_ruleJsonDS = alarm_ruleSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                if (StringUtils.isjson(s)) {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if ((!jsonObject.isEmpty()) && jsonObject.get("event_id") != null && jsonObject.get("rule_id") != null) {
                        collector.collect(jsonObject);
                    }
                }
            }
        }).setParallelism(updateCommonParallelism);

        //更新告警表
        Properties rulePro = new Properties();
        rulePro.setProperty("jdbc_driver", jdbc_driver);
        rulePro.setProperty("db_url_pattern", db_url_pattern);
        rulePro.setProperty("host", dorisHost);
        rulePro.setProperty("port", jdbc_port);
        rulePro.setProperty("db", dorisDB);
        rulePro.setProperty("user", dorisUser);
        rulePro.setProperty("passwd", dorisPw);
        String ruleSql = "update" + " sdc.event_alarm" + " set rule_id=? where event_id=?";
        String ruleStrings = "rule_id,event_id"; //按占位符?顺序
        alarm_ruleJsonDS.addSink(new JdbcDorisSink<>(ruleSql, ruleStrings, rulePro)).setParallelism(updateJdbcSinkParallelism);

        env.execute("personal to doris");

    }
}
