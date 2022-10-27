package brd.asset.flink.task;

import brd.asset.constants.ScanCollectConstant;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.entity.EventAlarm;
import brd.asset.flink.fun.*;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.common.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Properties;


/**
 * @program SecurityDataCenter
 * @description: 测试每个步骤
 * 注意：
 * 由于程序分布式执行，下面的properties不能复用
 * @author: 蒋青松
 * @create: 2022/09/06 17:18
 */
public class AssetProcessByStep {
    private static Logger log = Logger.getLogger(AssetProcessByStep.class);

    public static void main(String[] args) throws Exception {

        //String propPath = "D:\\DevelopData\\IDEAData\\securitydatacenter\\online\\src\\main\\resources\\asset_process.properties";
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String propPath = parameterTool.get("conf_path");
        //获取配置数据
        //------------------------------------获取配置数据 begin--------------------------------------
        //todo 注册全局参数，不需要每次传参
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String taskName = paramFromProps.get("task.name");
        String brokers = paramFromProps.get("consumer.bootstrap.server");
        String consumerTopic = paramFromProps.get("consumer.topic");
        String abnormalTopic = paramFromProps.get("abnormalTopic");
        String groupId = paramFromProps.get("consumer.groupId");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort");
        String dorisPort1 = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        String assetBaseTB = paramFromProps.get("dorisTable.assetBase");
        String assetTaskTB = paramFromProps.get("dorisTable.assetTask");
        String eventAlarmTB = paramFromProps.get("dorisTable.eventAlarm");
        String assetTaskOriginTB = paramFromProps.get("dorisTable.assetTaskOrigin");
        //ip数据库地址
        String ipDbPath = paramFromProps.get("ipDbPath");
        Integer openPortThreshold = paramFromProps.getInt("openPortThreshold");
        String processBlackList = paramFromProps.get("processBlackList");
        //Parallelism
        Integer commonParallelism = paramFromProps.getInt("assetProcess.commonParallelism");
        Integer kafkaParallelism = paramFromProps.getInt("assetProcess.kafkaParallelism");
        Integer dorisSinkParallelism = paramFromProps.getInt("assetProcess.dorisSinkParallelism");
        //------------------------------------获取配置数据 end--------------------------------------


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(commonParallelism);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        //step1: 读取原始数据&&过滤任务结束标志数据
        //kafka-source
        KafkaSource<String> source = KafkaUtil.getKafkaSource(brokers, consumerTopic, groupId);
        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source").setParallelism(kafkaParallelism);

        //DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.5.94", 7776);//todo 更改 kafka source
        final OutputTag<JSONObject> taskEndTag = new OutputTag<JSONObject>("task-end") {
        };
        //过滤任务结束标志数据
        SingleOutputStreamOperator<JSONObject> signedDS = kafkaSource.process(new FilterTaskEndProcess());
        DataStream<String> taskEndStream = signedDS.getSideOutput(taskEndTag).map(x -> x.getString(ScanCollectConstant.TASK_ID));

        //修改任务状态
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:mysql://" + dorisHost + ":" + dorisPort + "?useSSL=false");
        properties.setProperty("username", dorisUser);
        properties.setProperty("password", dorisPw);
        properties.setProperty("db", dorisDB);
        properties.setProperty("table", assetTaskTB);

        taskEndStream.process(new TaskEnd2DorisProcess(properties));

        //step4: json转AssetScan对象
        SingleOutputStreamOperator<AssetScanTask> assetOriginStream = signedDS.map(new Origin2AssetScan(/*ipDbPath*/));
        //step5: 拉宽原始数据入库
        SingleOutputStreamOperator<AssetScanTask> labeledAssetStream = assetOriginStream.map(new LabeledMap(dorisHost,
                dorisPort, dorisDB, dorisUser, dorisPw));

        Properties labeledProp = new Properties();
        labeledProp.setProperty("host", dorisHost);
        labeledProp.setProperty("port", dorisPort1);
        labeledProp.setProperty("username", dorisUser);
        labeledProp.setProperty("password", dorisPw);
        labeledProp.setProperty("db", dorisDB);
        labeledProp.setProperty("table", assetTaskOriginTB);
        labeledProp.setProperty("labelPrefix", "task-origin-" + System.currentTimeMillis());

        AssetDataCommonSink assetScanOriginSink = new AssetDataCommonSink(env, labeledAssetStream, labeledProp, dorisSinkParallelism);
        assetScanOriginSink.sink();

        //step6:异常资产&&更新asset_base表
        SingleOutputStreamOperator<EventAlarm> abnormalAnalysisDS = labeledAssetStream
                .process(new AbnormalAndLabelProcess1(openPortThreshold, processBlackList, dorisHost, dorisPort, dorisDB, dorisUser, dorisPw));


        //step7: 资产入库（insert or update）
        final OutputTag<AssetScanTask> insertTag = new OutputTag<AssetScanTask>("asset-base-insert") {
        };
        final OutputTag<AssetScanTask> updateTag = new OutputTag<AssetScanTask>("asset-base-update") {
        };

        //AssetScanTask 转成 AssetBase类型
        DataStream<AssetScanTask> insertAssetBaseDs = abnormalAnalysisDS.getSideOutput(insertTag);
        DataStream<AssetScanTask> updateAssetBaseDs = abnormalAnalysisDS.getSideOutput(updateTag);

        //插入资产基础表
        Properties assetbaseProp = new Properties();
        assetbaseProp.setProperty("host", dorisHost);
        assetbaseProp.setProperty("port", dorisPort1);
        assetbaseProp.setProperty("username", dorisUser);
        assetbaseProp.setProperty("password", dorisPw);
        assetbaseProp.setProperty("db", dorisDB);
        assetbaseProp.setProperty("table", assetBaseTB);
        assetbaseProp.setProperty("labelPrefix", "asset-base-" + System.currentTimeMillis());
        SingleOutputStreamOperator<AssetBase> insertAssetBaseFinalDS = insertAssetBaseDs.map(new AssetScanTask2AssetBaseMap());
        AssetDataCommonSink assetBaseSink = new AssetDataCommonSink(env, insertAssetBaseFinalDS, assetbaseProp, dorisSinkParallelism);
        assetBaseSink.sink();

        //修改资产基础表
        Properties assetUpdateProp = new Properties();
        assetUpdateProp.setProperty("url", "jdbc:mysql://" + dorisHost + ":" + dorisPort + "?useSSL=false");
        assetUpdateProp.setProperty("username", dorisUser);
        assetUpdateProp.setProperty("password", dorisPw);
        assetUpdateProp.setProperty("db", dorisDB);
        assetUpdateProp.setProperty("table", assetBaseTB);
        updateAssetBaseDs.map(new AssetScanTask2AssetBaseMap()).process(new AssetbaseUpdate2DorisProcess(assetUpdateProp));

        //step8: 异常资产告警入库
        Properties eventAlarmProp = new Properties();
        eventAlarmProp.setProperty("host", dorisHost);
        eventAlarmProp.setProperty("port", dorisPort1);
        eventAlarmProp.setProperty("username", dorisUser);
        eventAlarmProp.setProperty("password", dorisPw);
        eventAlarmProp.setProperty("db", dorisDB);
        eventAlarmProp.setProperty("table", eventAlarmTB);
        eventAlarmProp.setProperty("labelPrefix", "event-alarm-" + System.currentTimeMillis());
        AssetDataCommonSink eventAlarmSink = new AssetDataCommonSink(env, abnormalAnalysisDS, eventAlarmProp, dorisSinkParallelism);

        //todo  把abnormalAnalysisDS  to json 入kafka
        SingleOutputStreamOperator<String> abnormalAnalysisJsonDS = abnormalAnalysisDS.map(v -> JSONObject.toJSONString(v));
        abnormalAnalysisJsonDS.sinkTo(KafkaUtil.getKafkaSink(brokers, abnormalTopic));

        eventAlarmSink.sink();
        env.execute("中移: asset task.");
    }

}
