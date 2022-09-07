package brd.asset.flink.task;

import brd.asset.constants.AlarmItem;
import brd.asset.constants.ScanCollectConstant;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.flink.fun.*;
import brd.asset.flink.sink.AssetScanOriginSink;
import brd.asset.flink.sink.DorisSinkBase;
import brd.asset.flink.source.AssetBaseSource;
import brd.common.FlinkUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;


/**
 * @program SecurityDataCenter
 * @description: 测试每个步骤
 * @author: 蒋青松
 * @create: 2022/09/06 17:18
 */
public class AssetProcessStepTest {
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

        //step1: 读取资产基础表
        DataStreamSource<AssetBase> assetBaseDS = env.addSource(new AssetBaseSource(dorisHost, dorisPort, dorisDB, assetBaseTB, dorisUser, dorisPw));
        assetBaseDS.print("base: ");

        //step2: 读取原始数据&&过滤任务结束标志数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.5.94", 7776);//todo 更改 kafka source
        socketTextStream.print();

        final OutputTag<JSONObject> taskEndTag = new OutputTag<JSONObject>("task-end") {
        };
        //过滤任务结束标志数据
        SingleOutputStreamOperator<JSONObject> signedDS = socketTextStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject originObject = JSON.parseObject(value);
                //resource info
                JSONObject resourceObj = JSON.parseObject(originObject.get(ScanCollectConstant.RESOURCE_INFO).toString());
                if (resourceObj.size() == 0) {
                    ctx.output(taskEndTag, originObject);
                } else {
                    out.collect(originObject);
                }
            }
        });
        DataStream<String> taskEndStream = signedDS.getSideOutput(taskEndTag).map(x -> x.getString(ScanCollectConstant.TASK_ID));
        //修改任务状态
        Properties properties = new Properties();
        properties.setProperty("url", "jdbc:mysql://" + dorisHost + ":" + dorisPort);
        properties.setProperty("username", dorisUser);
        properties.setProperty("password", dorisPw);
        properties.setProperty("db", dorisDB);
        properties.setProperty("table", assetTaskTB);

        taskEndStream.process(new TaskEnd2DorisProcess(properties));


        //step3: 基础表数据广播
        final MapStateDescriptor<String, AssetBase> assetBaseMapStateDescriptor = new MapStateDescriptor<>(
                "assetBaseState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<AssetBase>() {
        }));
        BroadcastStream<AssetBase> assetBaseBroadcastDS = assetBaseDS.broadcast(assetBaseMapStateDescriptor);

        //step4: json转AssetScan对象
        SingleOutputStreamOperator<AssetScanTask> assetOriginStream = signedDS.map(new Origin2AssetScan(ipDbPath));
        assetOriginStream.print("json转AssetScan: ");

        //step5: 拉宽原始数据入库
        SingleOutputStreamOperator<AssetScanTask> labeledAssetStream = assetOriginStream.map(new LabeledMap(dorisHost,
                dorisPort, dorisDB, dorisUser, dorisPw));
        labeledAssetStream.print("labeled data: ");
        Properties labeledProp = new Properties();
        labeledProp.setProperty("host", dorisHost);
        labeledProp.setProperty("port", dorisPort);
        labeledProp.setProperty("username", dorisUser);
        labeledProp.setProperty("password", dorisPw);
        labeledProp.setProperty("db", dorisDB);
        labeledProp.setProperty("table", assetTaskOriginTB);
        labeledProp.setProperty("batchSize", batchSize.toString());
        AssetScanOriginSink assetScanOriginSink = new AssetScanOriginSink(env, labeledAssetStream, labeledProp);
        assetScanOriginSink.sink();

        //step6: 宽表和asset_base表关联并分流：异常资产&&更新asset_base表
        labeledAssetStream.connect(assetBaseBroadcastDS)
                .process(new AbnormalAndLabelProcess(assetBaseMapStateDescriptor, openPortThreshold, processBlackList));
        //异常资产告警入库
        //abnormalAndLabeledDS.addSink(AbnormalAssetSink.getSink()); //todo 入库告警表


        env.execute("step test.");

    }

}
