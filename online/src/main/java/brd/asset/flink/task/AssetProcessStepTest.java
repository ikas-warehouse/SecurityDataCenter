package brd.asset.flink.task;

import brd.asset.constants.ScanCollectConstant;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.flink.fun.*;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.asset.flink.source.AssetBaseSource;
import brd.common.FlinkUtils;
import brd.common.StringUtils;
import brd.common.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * @program SecurityDataCenter
 * @description: 测试每个步骤
 * @author: 蒋青松
 * @create: 2022/09/06 17:18
 */
public class AssetProcessStepTest {
    private static Logger log = Logger.getLogger(AssetProcessStepTest.class);

    public static void main(String[] args) throws Exception {

        String propPath = "/Users/mac/dev/brd_2021/SecurityDataCenter/online/src/main/resources/asset_process.properties";
        //获取配置数据
        ParameterTool paramFromProps = ParameterTool.fromPropertiesFile(propPath);
        String taskName = paramFromProps.get("task.name");
        String consumerTopic = paramFromProps.get("consumer.topic");
        String groupId = paramFromProps.get("consumer.groupId");
        Long timeout = paramFromProps.getLong("timeout");
        //doris properties
        String dorisHost = paramFromProps.get("dorisHost");
        String dorisPort = paramFromProps.get("dorisPort");
        String dorisPort1 = paramFromProps.get("dorisPort1");
        String dorisUser = paramFromProps.get("dorisUser");
        String dorisPw = paramFromProps.get("dorisPw");
        String dorisDB = paramFromProps.get("dorisDB");
        String assetBaseTB = paramFromProps.get("dorisTable.assetBase");
        String assetTaskTB = paramFromProps.get("dorisTable.assetTask");
        String assetTaskOriginTB = paramFromProps.get("dorisTable.assetTaskOrigin");
        Integer batchSize = Integer.valueOf(paramFromProps.get("batchSize"));
        Long batchIntervalMs = Long.valueOf(paramFromProps.get("batchIntervalMs"));

        //ip数据库地址
        String ipDbPath = paramFromProps.get("ipDbPath");

        Integer openPortThreshold = paramFromProps.getInt("openPortThreshold");
        String processBlackList = paramFromProps.get("processBlackList");

        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);
        env.enableCheckpointing(10000L);

        //step1: 读取资产基础表
        DataStreamSource<AssetBase> assetBaseDS = env.addSource(new AssetBaseSource(dorisHost, dorisPort, dorisDB, assetBaseTB, dorisUser, dorisPw));
        //assetBaseDS.print("base: ");

        //step2: 读取原始数据&&过滤任务结束标志数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.5.94", 7776);//todo 更改 kafka source
        socketTextStream.print();

        final OutputTag<JSONObject> taskEndTag = new OutputTag<JSONObject>("task-end") {
        };
        //过滤任务结束标志数据
        SingleOutputStreamOperator<JSONObject> signedDS = socketTextStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                boolean isjson = StringUtils.isjson(value);
                if (isjson) {
                    JSONObject originObject = JSON.parseObject(value);
                    //resource info
                    try {
                        JSONObject resourceObj = JSON.parseObject(originObject.get(ScanCollectConstant.RESOURCE_INFO).toString());
                        if (resourceObj.size() == 0) {
                            ctx.output(taskEndTag, originObject);
                        } else {
                            out.collect(originObject);
                        }
                    } catch (Exception e) {
                        log.error("输入内容有误，mas=" + e.getMessage());
                    }

                }
            }
        });
        DataStream<String> taskEndStream = signedDS.getSideOutput(taskEndTag).map(x -> x.getString(ScanCollectConstant.TASK_ID));
        //修改任务状态
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:mysql://" + dorisHost + ":" + dorisPort + "?useSSL=false");
        properties.setProperty("username", dorisUser);
        properties.setProperty("password", dorisPw);
        properties.setProperty("db", dorisDB);
        properties.setProperty("table", assetTaskTB);
        taskEndStream.print("task-end: ");

        taskEndStream.process(new TaskEnd2DorisProcess(properties));


        //step3: 基础表数据广播
        final MapStateDescriptor<String, AssetBase> assetBaseMapStateDescriptor = new MapStateDescriptor<>(
                "assetBaseState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<AssetBase>() {
        }));
        BroadcastStream<AssetBase> assetBaseBroadcastDS = assetBaseDS.broadcast(assetBaseMapStateDescriptor);

        //step4: json转AssetScan对象
        SingleOutputStreamOperator<AssetScanTask> assetOriginStream = signedDS.map(new Origin2AssetScan(ipDbPath));

        //step5: 拉宽原始数据入库
        SingleOutputStreamOperator<AssetScanTask> labeledAssetStream = assetOriginStream.map(new LabeledMap(dorisHost,
                dorisPort, dorisDB, dorisUser, dorisPw));
        labeledAssetStream.print("labeled data: ");
        Properties labeledProp = new Properties();
        labeledProp.setProperty("host", dorisHost);
        labeledProp.setProperty("port", dorisPort1);
        labeledProp.setProperty("username", dorisUser);
        labeledProp.setProperty("password", dorisPw);
        labeledProp.setProperty("db", dorisDB);
        labeledProp.setProperty("table", assetTaskOriginTB);
        labeledProp.setProperty("labelPrefix", "task-origin-" + System.currentTimeMillis());

        AssetDataCommonSink assetScanOriginSink = new AssetDataCommonSink(env, labeledAssetStream, labeledProp);
        assetScanOriginSink.sink();

        //step6: 宽表和asset_base表关联并分流：异常资产&&更新asset_base表
        SingleOutputStreamOperator<String> abnormalAnalysisDS = labeledAssetStream.connect(assetBaseBroadcastDS)
                .process(new AbnormalAndLabelProcess(assetBaseMapStateDescriptor, openPortThreshold, processBlackList));

        //step7: 资产入库（insert or update）
        final OutputTag<AssetScanTask> insertTag = new OutputTag<AssetScanTask>("asset-base-insert") {
        };
        final OutputTag<AssetScanTask> updateTag = new OutputTag<AssetScanTask>("asset-base-update") {
        };

        //AssetScanTask 转成 AssetBase类型
        DataStream<AssetScanTask> insertAssetBaseDs = abnormalAnalysisDS.getSideOutput(insertTag);
        insertAssetBaseDs.print("insert asset base: ");
        DataStream<AssetScanTask> updateAssetBaseDs = abnormalAnalysisDS.getSideOutput(updateTag);
        updateAssetBaseDs.print("update asset base: ");

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
        insertAssetBaseFinalDS.print("insert final");
        AssetDataCommonSink assetBaseSink = new AssetDataCommonSink(env, insertAssetBaseFinalDS, assetbaseProp);
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

        env.execute("step test.");
    }

}
