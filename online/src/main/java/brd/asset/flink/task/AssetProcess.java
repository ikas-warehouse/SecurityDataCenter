package brd.asset.flink.task;

import brd.asset.constants.ScanCollectConstant;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.flink.fun.LabeledMap;
import brd.asset.flink.fun.Origin2AssetScan;
import brd.asset.flink.sink.AssetDataCommonSink;
import brd.asset.flink.source.AssetBaseSource;
import brd.asset.pojo.AssetScanOrigin;
import brd.common.FlinkUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @Author: leo.j
 * @desc: 态感资产数据实时处理：
 * 1、
 * @Date: 2022/3/25 10:52 上午
 * */



public class AssetProcess {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        //String propPath = parameterTool.get("conf_path");
        String propPath = "/Users/mac/dev/brd_2021/SecurityDataCenter/online/src/main/resources/asset_process.properties";

        /**
         *  this.HOST = HOST;
         *         this.PORT = PORT;
         *         this.DB = DB;
         *         this.USER = USER;
         *         this.PASSWD = PASSWD;
         */
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
        String assetTaskOriginTB = paramFromProps.get("dorisTable.assetTaskOrigin");
        Integer batchSize = Integer.valueOf(paramFromProps.get("batchSize"));
        Long batchIntervalMs = Long.valueOf(paramFromProps.get("batchIntervalMs"));

        //ip数据库地址
        String ipDbPath = paramFromProps.get("ipDbPath");

        Integer openPortThreshold = paramFromProps.getInt("openPortThreshold");
        String processBlackList = paramFromProps.get("processBlackList");


        //todo  做checkpoint&&stateBackend
        final StreamExecutionEnvironment env = FlinkUtils.getEnv();
        //将 ParameterTool 注册为全作业参数的参数
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        //用socket测试
        //todo socket 改 kafka
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.5.94", 7776);
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
        DataStream<JSONObject> taskEndStream = signedDS.getSideOutput(taskEndTag);
        //任务结束更新状态
        SingleOutputStreamOperator<String> taskIdEndStream = taskEndStream.map(data -> data.getString(ScanCollectConstant.TASK_ID));
        taskIdEndStream.print();
        //TODO sink doris
        //taskIdEndStream.addSink(TaskEndSink.getSink());

        //flink cdc 同步asset_base数据 todo doris获取base信息

        //3.2 获取资产基础表数据 全量+增量
        DataStreamSource<AssetBase> assetBaseDS = env.addSource(new AssetBaseSource(dorisHost, dorisPort, dorisDB, assetBaseTB, dorisUser, dorisPw));
        //assetBaseDS.print("flink cdc: ");
        //assetBaseDS做广播状态 assetBaseStateDescriptor
        final MapStateDescriptor<String, AssetBase> assetBaseMapStateDescriptor = new MapStateDescriptor<>(
                "assetBaseState", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<AssetBase>() {
        }));
        BroadcastStream<AssetBase> assetBaseBroadcastDS = assetBaseDS.broadcast(assetBaseMapStateDescriptor);

        //json转AssetScan对象
        SingleOutputStreamOperator<AssetScanTask> assetOriginStream = signedDS.map(new Origin2AssetScan(ipDbPath));

        //拉宽原始数据入库
        SingleOutputStreamOperator<AssetScanTask> labeledAssetStream = assetOriginStream.map(new LabeledMap(dorisHost, dorisPort, dorisDB, dorisUser, dorisPw));
        Properties labeledProp = new Properties();
        labeledProp.setProperty("url", "jdbc:mysql://" + dorisHost + ":" + dorisPort);
        labeledProp.setProperty("host", dorisHost);
        labeledProp.setProperty("port", dorisPort);
        labeledProp.setProperty("username", dorisUser);
        labeledProp.setProperty("password", dorisPw);
        labeledProp.setProperty("db", dorisDB);
        labeledProp.setProperty("table", assetTaskOriginTB);
        labeledProp.setProperty("batchSize", batchSize.toString());
        AssetDataCommonSink assetScanOriginSink = new AssetDataCommonSink(env, labeledAssetStream, labeledProp);
        assetScanOriginSink.sink();

        //宽表和asset_base表关联并分流：异常资产&&更新asset_base表
        /*SingleOutputStreamOperator<Tuple3<String, String, AlarmItem>> abnormalAndLabeledDS = labeledAssetStream.connect(assetBaseBroadcastDS)
                .process(new AbnormalAndLabelProcess(assetBaseMapStateDescriptor, openPortThreshold, processBlackList));*/
        //异常资产告警入库
        //abnormalAndLabeledDS.addSink(AbnormalAssetSink.getSink()); //todo 入库告警表


        //更新asset_base表
        final OutputTag<Tuple2<AssetScanOrigin, String>> insertTag = new OutputTag<Tuple2<AssetScanOrigin, String>>("insert-tag") {};
        final OutputTag<Tuple2<AssetScanOrigin, String>> updateTag = new OutputTag<Tuple2<AssetScanOrigin, String>>("update-tag") {};

        //DataStream<Tuple2<AssetScanOrigin, String>> insertAssetBaseDS = abnormalAndLabeledDS.getSideOutput(insertTag);
        //DataStream<Tuple2<AssetScanOrigin, String>> updateAssetBaseDS = abnormalAndLabeledDS.getSideOutput(updateTag);
        //insertAssetBaseDS.addSink(AssetBaseSink.getInsertSink());
        //updateAssetBaseDS.addSink(AssetBaseSink.getUpdateSink());

        env.execute(taskName);
    }

}
