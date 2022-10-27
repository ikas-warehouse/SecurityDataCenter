package brd.asset.flink.task;

import brd.asset.flink.fun.JsonFilterFunction;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.asset.flink.sink.KafkaDorisSink;
import brd.common.KafkaUtil;
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
        String vulnerTopic = paramFromProps.get("vulnerTopic");
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
        //泛化表 表 字段 
        String formatTb = "event_format";
        String formatStrings = "id,accept_time,evt_time,evt_id,evt_name,evt_type,evt_subtype,evt_level,evt_description,attack_type,attack_brief,starttime,endtime,duration,protocol,dev_type,dev_name,dev_ip,dev_vendor,app_name,app_version,os,os_version,src_ip,src_port,src_ip_type,dest_ip,dest_port,dest_ip_type,src_mac,dest_mac,user,login_method,result,errorcode,process,src_ip_country,src_ip_province,src_ip_city,src_ip_county,src_ip_isp,src_ip_longitude,src_ip_latitude,dest_ip_country,dest_ip_province,dest_ip_city,dest_ip_county,dest_ip_isp,dest_ip_longitude,dest_ip_latitude,ul_octets,ul_packets,dl_octets,dl_packets,sum_times,log_type,original_id";

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
        formatPro.setProperty("strings", formatStrings);
        formatPro.setProperty("labelPrefix", "event-format-" + System.currentTimeMillis());
        KafkaDorisSink formatSink = new KafkaDorisSink(env, formatPro, kafkaParallelism, dorisSinkParallelism);
        formatSink.sink();
        
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

        //-----------------------------------------------------------------------------------
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

        env.execute("data to doris");

    }
}
