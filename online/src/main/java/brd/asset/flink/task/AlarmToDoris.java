package brd.asset.flink.task;

import brd.asset.flink.sink.JdbcDorisSink;
import brd.asset.flink.sink.KafkaDorisSink;
import brd.common.KafkaUtils;
import brd.common.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @program AlarmToDoris
 * @description: 1.日志告警表入库
 * 2.监听kafka修改告警表 处理字段 & 处理时间字段
 * 3.监听kafka修改告警表 规则字段
 * @author: 张世钰
 * @create: 2022/10/12 10:37
 */
public class AlarmToDoris {
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
        String alarmTopic = paramFromProps.get("alarmTopic");
        String handleTopic = paramFromProps.get("handleTopic");
        String ruleTopic = paramFromProps.get("ruleTopic");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        //doris tables
        String alarmTb = paramFromProps.get("dorisTable.eventAlarm");
        String alarmField = paramFromProps.get("alarmField");
        String alarmKey = paramFromProps.get("alarmKey");
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
        //告警表入库
        Properties alarmPro = new Properties();
        alarmPro.setProperty("brokers", brokers);
        alarmPro.setProperty("topic", alarmTopic);
        alarmPro.setProperty("groupId", groupId);
        alarmPro.setProperty("host", dorisHost);
        alarmPro.setProperty("port", dorisPort);
        alarmPro.setProperty("db", dorisDB);
        alarmPro.setProperty("username", dorisUser);
        alarmPro.setProperty("password", dorisPw);
        alarmPro.setProperty("table", alarmTb);
        alarmPro.setProperty("fieldString", alarmField);
        alarmPro.setProperty("keyString", alarmKey);
        alarmPro.setProperty("labelPrefix", "event-alarm-" + System.currentTimeMillis());
        KafkaDorisSink alarmSink = new KafkaDorisSink(env, alarmPro, kafkaParallelism, dorisSinkParallelism);
        alarmSink.sink();

        //----------------------------------------------------------------------------------------
        //获取告警处理信息 alarm_handle-kafka-source
        KafkaSource<String> alarmHandleKafkaSource = KafkaUtils.getKafkaSource(brokers, handleTopic, groupId);
        DataStreamSource<String> alarmHandleSource = env.fromSource(alarmHandleKafkaSource, WatermarkStrategy.noWatermarks(), "alarm_handle-kafka-source").setParallelism(updateKafkaParallelism);

        //转型JSONObject 添加:处理字段 时间字段
        SingleOutputStreamOperator<JSONObject> alarmHandleJsonDS = alarmHandleSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String jsonStr, Collector<JSONObject> collector) throws Exception {
                if (StringUtils.isjson(jsonStr) && jsonStr != null && jsonStr.replaceAll("\\s*", "").length() != 0) {
                    JSONObject jsonObject = JSON.parseObject(jsonStr);
                    if (jsonObject.get("event_id") != null) {
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

        String handleSql = "update " + dorisDB + "." + alarmTb + " set handle=?, handle_time=? where event_id=?";
        String handleStrings = "handle,handle_time,event_id"; //按占位符?顺序
        alarmHandleJsonDS.addSink(new JdbcDorisSink<>(handleSql, handleStrings, handlePro)).setParallelism(updateJdbcSinkParallelism).name("handle-update-sink");
        
        //-------------------------------------------------------------------------------
        //读取告警规则信息 alarm_rule-kafka-source
        KafkaSource<String> alarmRuleKafkaSource = KafkaUtils.getKafkaSource(brokers, ruleTopic, groupId);
        DataStreamSource<String> alarmRuleSource = env.fromSource(alarmRuleKafkaSource, WatermarkStrategy.noWatermarks(), "alarm_rule-kafka-source").setParallelism(updateKafkaParallelism);

        //转型成JSONObject 添加 处理字段 处理事件字段
        SingleOutputStreamOperator<JSONObject> alarmRuleJsonDS = alarmRuleSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String jsonStr, Collector<JSONObject> collector) throws Exception {
                if (StringUtils.isjson(jsonStr) && jsonStr != null && jsonStr.replaceAll("\\s*", "").length() != 0) {
                    JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                    if (jsonObject.get("event_id") != null && jsonObject.get("rule_id") != null) {
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
        String ruleSql = "update " + dorisDB + "." + alarmTb + " set rule_id=? where event_id=?";
        String ruleStrings = "rule_id,event_id"; //按占位符?顺序
        alarmRuleJsonDS.addSink(new JdbcDorisSink<>(ruleSql, ruleStrings, rulePro)).setParallelism(updateJdbcSinkParallelism).name("rule-update-sink");

        env.execute("alarm to doris");

    }
}
