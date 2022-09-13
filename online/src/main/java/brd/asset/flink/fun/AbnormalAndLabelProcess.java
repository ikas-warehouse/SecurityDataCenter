package brd.asset.flink.fun;

import brd.asset.constants.AlarmItem;
import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.asset.entity.EventAlarm;
import brd.asset.pojo.OpenServiceOfPort;
import brd.common.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * @Author: leo.j
 * @desc: 1、异常资产评估
 * 2、资产基础表更新数据侧输出
 * @Date: 2022/4/1 11:00 上午
 */
public class AbnormalAndLabelProcess extends BroadcastProcessFunction<AssetScanTask, AssetBase, EventAlarm> {

    //纳管资产
    private Set<String> accessAssets = new HashSet<>();
    //资产基本表
    private Map<String, AssetBase> unique2baseMap = new HashMap<>();

    private MapStateDescriptor<String, AssetBase> assetBaseMapStateDescriptor;

    private static final String UNIQUE_CONNECT_STR = "_";

    private int openPortThreshold;
    private List<String> processBlackList;

    public AbnormalAndLabelProcess(MapStateDescriptor<String, AssetBase> assetBaseMapStateDescriptor, int openPortThreshold, String processBlack) {
        this.assetBaseMapStateDescriptor = assetBaseMapStateDescriptor;
        this.openPortThreshold = openPortThreshold;
        if ("".equals(processBlack)) {
            this.processBlackList = Arrays.asList();
        } else {
            this.processBlackList = Arrays.asList(processBlack.split(","));
        }
    }

    @Override
    public void processElement(AssetScanTask asset, ReadOnlyContext ctx, Collector<EventAlarm> out) throws Exception {

        OutputTag<AssetScanTask> insertTag = new OutputTag<AssetScanTask>("asset-base-insert") {
        };
        OutputTag<AssetScanTask> updateTag = new OutputTag<AssetScanTask>("asset-base-update") {
        };

        ReadOnlyBroadcastState<String, AssetBase> assetBaseBroadcastState = ctx.getBroadcastState(assetBaseMapStateDescriptor);
        //数据更新
        unique2baseMap.clear();
        assetBaseBroadcastState.immutableEntries().iterator().forEachRemaining(i -> {
            String ip = i.getValue().getDevice_ip();
            String mac = i.getValue().getDevice_mac();
            String unique = ip + UNIQUE_CONNECT_STR + mac;
            AssetBase assetBase = i.getValue();
            unique2baseMap.put(unique, assetBase);
            //accessAssets.add(ip);
        });
        getAccessAssets();

        //获取所有资产ip列表
        Set<String> assets = unique2baseMap.keySet();
        String ip = asset.getDevice_ip();
        String mac = asset.getDevice_mac();
        String assetUnique = ip + UNIQUE_CONNECT_STR + mac;
        // 资产基础表新增或更新数据输出
        if (assets.contains(assetUnique)) {//ip+mac作为资产唯一性
            //更新
            ctx.output(updateTag, asset);
        } else {
            //新增
            ctx.output(insertTag, asset);
        }

        //-------- 异常资产评估 --------
        //1.异常开放端口
        List<OpenServiceOfPort> openServiceOfPorts = JSON.parseArray(asset.getOpen_service_of_port(), OpenServiceOfPort.class);
        if (openServiceOfPorts.size() > openPortThreshold) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_OPEN_PORT.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_OPEN_PORT.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);

        }
        //2.异常资产信息变更(对同一台资产os info，与上一次资产采集的信息对比)
        if (assets.contains(assetUnique)) {
            AssetBase assetBase = unique2baseMap.get(assetUnique);
            String osInfo = assetBase.getOs_info();
            if (osInfo != null && !osInfo.equals(asset.getOs_info())) {
                EventAlarm alarm = new EventAlarm();
                alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
                alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_CHANGE.getEventDesc());
                alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_CHANGE.getEventId());
                alarm.setEvent_level("中");
                alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
                alarm.setEvent_dev_ip(ip);
                alarm.setEvent_dev_mac(mac);
                out.collect(alarm);
            }
        }

        //4.发现无主资产
        if (assets.contains(assetUnique)) {
            AssetBase lastAsset = unique2baseMap.get(assetUnique);
            String attributionGroup = lastAsset.getResponsible_group();
            String responsiblePerson = lastAsset.getResponsible_person();
            if (attributionGroup == null || "".equals(attributionGroup) || responsiblePerson == null || "".equals(responsiblePerson)) {
                EventAlarm alarm = new EventAlarm();
                alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
                alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_NO_GROUP.getEventDesc());
                alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_NO_GROUP.getEventId());
                alarm.setEvent_level("中");
                alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
                alarm.setEvent_dev_ip(ip);
                alarm.setEvent_dev_mac(mac);
                out.collect(alarm);
            }
        }
        //5.发现未知资产
        if (!accessAssets.contains(assetUnique)) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_ASSET_UNKNOWN.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_ASSET_UNKNOWN.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);
        }
        //6.异常进程信息
        if (processBlackList.contains(ip)) {
            EventAlarm alarm = new EventAlarm();
            alarm.setEvent_id(UUID.randomUUID().toString().replaceAll("-", ""));
            alarm.setEvent_title(AlarmItem.ABNORMAL_PROESS.getEventDesc());
            alarm.setEvent_type(AlarmItem.ABNORMAL_PROESS.getEventId());
            alarm.setEvent_level("中");
            alarm.setEvent_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));
            alarm.setEvent_dev_ip(ip);
            alarm.setEvent_dev_mac(mac);
            out.collect(alarm);
        }
    }

    @Override
    public void processBroadcastElement(AssetBase value, Context ctx, Collector<EventAlarm> out) throws Exception {
        BroadcastState<String, AssetBase> assetBaseBroadcastState = ctx.getBroadcastState(assetBaseMapStateDescriptor);
        assetBaseBroadcastState.put(value.getDevice_ip() + UNIQUE_CONNECT_STR + value.getDevice_mac(), value);
    }

    /**
     * 获取纳管资产
     */
    public void getAccessAssets() {
        boolean isClear = false;
        for (AssetBase assetBase : unique2baseMap.values()) {
            if (!isClear) {
                accessAssets.clear();
                isClear = true;
            }
            //过滤纳管资产ip
            if ("_PERMITTED".equals(assetBase.getPermitted())) {
                String ip = assetBase.getDevice_ip();
                String mac = assetBase.getDevice_mac();
                String unique = ip + ":" + mac;
                accessAssets.add(unique);
            }
        }
    }
}
