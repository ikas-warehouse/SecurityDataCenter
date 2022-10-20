package brd.asset.flink.task;

import brd.asset.flink.sink.KafkaDorisSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @program CommonKafkaToDoris
 * @description: 通用kafka数据导入doris
 * @author: 张世钰
 * @create: 2022/10/20 13:00
 */
public class CommonKafkaToDoris {
    public static void main(String[] args) throws Exception {
        //获取配置数据
        String propPath = "D:\\DevelopData\\IDEAData\\securitydatacenter\\online\\src\\main\\resources\\common_import.properties";
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //String propPath = parameterTool.get("conf_path");
        //------------------------------------获取配置数据 begin--------------------------------------
        //kafka properties
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String brokers = paramFromProps.get("consumer.bootstrap.server");
        String groupId = paramFromProps.get("consumer.groupId");
        //kafka topic
        String alarmTopic = paramFromProps.get("commonTopic");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        String dorisTb = paramFromProps.get("dorisTb");
        String fields = paramFromProps.get("fields");
        String labelPrefix = paramFromProps.get("labelPrefix");
        //parallelism
        Integer commonParallelism = paramFromProps.getInt("commonParallelism");
        Integer kafkaParallelism = paramFromProps.getInt("kafkaParallelism");
        Integer dorisSinkParallelism = paramFromProps.getInt("dorisSinkParallelism");
        //------------------------------------获取配置数据 end--------------------------------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(commonParallelism);
        env.enableCheckpointing(5000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //------------------------------------------------------------------------------------------

        //入库
        Properties alarmPro = new Properties();
        alarmPro.setProperty("brokers", brokers);
        alarmPro.setProperty("topic", alarmTopic);
        alarmPro.setProperty("groupId", groupId);
        alarmPro.setProperty("host", dorisHost);
        alarmPro.setProperty("port", dorisPort);
        alarmPro.setProperty("db", dorisDB);
        alarmPro.setProperty("username", dorisUser);
        alarmPro.setProperty("password", dorisPw);
        alarmPro.setProperty("table", dorisTb);
        alarmPro.setProperty("strings", fields);
        alarmPro.setProperty("labelPrefix", labelPrefix + System.currentTimeMillis());
        KafkaDorisSink alarmSink = new KafkaDorisSink(env, alarmPro, kafkaParallelism, dorisSinkParallelism);
        alarmSink.sink();

        env.execute(dorisDB + "." + dorisTb + " - import job");
    }
}
