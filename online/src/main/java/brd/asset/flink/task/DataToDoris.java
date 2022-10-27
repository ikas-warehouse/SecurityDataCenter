package brd.asset.flink.task;

import brd.asset.flink.sink.KafkaDorisSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @program DataToDoris
 * @description: 1. 告警表表入库
 * 2. cve漏洞表入库
 * 3. cnvd漏洞表入库
 * 4. 威胁情报表入库
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
        String alarmTopic = paramFromProps.get("alarmTopic");
        String cveTopic = paramFromProps.get("cveTopic");
        String cnvdTopic = paramFromProps.get("cnvdTopic");
        String tjTopic = paramFromProps.get("tjTopic");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
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
        //告警表 表 字段 
        String alarmTb = "event_alarm";
        String alarmStrings = "event_id,event_title,event_type,event_level,event_time,event_count,event_dev_ip,event_dev_mac,event_dev_type,event_device_factory,event_device_model,event_device_name,event_source_ip,event_source_port,event_source_adress,event_target_ip,event_target_port,event_target_adress,event_affected_dev,event_affected_dev_belong,event_description,trace_log_ids,protocol,traffic_size,file_name,handle,handle_time";

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
        alarmPro.setProperty("strings", alarmStrings);
        alarmPro.setProperty("labelPrefix", "event-alarm-" + System.currentTimeMillis());
        KafkaDorisSink alarmSink = new KafkaDorisSink(env, alarmPro, kafkaParallelism, dorisSinkParallelism);
        alarmSink.sink();

        //------------------------------------------------------------------------------------------
        //cve漏洞表 表 字段 
        String cveTb = "cve_vulnerability";
        String cveStrings = "number,status,description,reference,phase,votes,comments,update_time";

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
        cvePro.setProperty("strings", cveStrings);
        cvePro.setProperty("labelPrefix", "cve-vulnerability-" + System.currentTimeMillis());
        KafkaDorisSink cveSink = new KafkaDorisSink(env, cvePro, kafkaParallelism, dorisSinkParallelism);
        cveSink.sink();

        //------------------------------------------------------------------------------------------
        //cnvd漏洞表 表 字段 
        String cnvdTb = "cnvd_vulnerability";
        String cnvdStrings = "number,cve_number,cve_url,title,serverity,product,is_event,submit_time,open_time,reference_link,formalway,description,patchname,patch_description,update_time";

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
        cnvdPro.setProperty("strings", cnvdStrings);
        cnvdPro.setProperty("labelPrefix", "cnvd-vulnerability-" + System.currentTimeMillis());
        KafkaDorisSink cnvdSink = new KafkaDorisSink(env, cnvdPro, kafkaParallelism, dorisSinkParallelism);
        cnvdSink.sink();

        //------------------------------------------------------------------------------------------
        //天际情报表 表 字段 
        String tjTb = "tj_threat";
        String tjStrings = "type,value,geo,reputation,in_time";

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
        tjPro.setProperty("strings", tjStrings);
        tjPro.setProperty("labelPrefix", "tj-threat-" + System.currentTimeMillis());
        KafkaDorisSink tjSink = new KafkaDorisSink(env, tjPro, kafkaParallelism, dorisSinkParallelism);
        tjSink.sink();

        env.execute("data to doris");

    }
}
