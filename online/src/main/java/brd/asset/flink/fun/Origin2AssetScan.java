package brd.asset.flink.fun;

import brd.asset.constants.ScanCollectConstant;
import brd.asset.entity.AssetScanTask;
import brd.asset.pojo.DataBaseInfoScan;
import brd.asset.pojo.MessageOrientedMiddlewareScan;
import brd.asset.pojo.OpenServiceOfPort;
import brd.common.IpLocation;
import brd.common.Location;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.log4j.Logger;

import java.util.List;


/**
 * @Author: leo.j
 * @desc: 原始资产数据转AssetScan对象
 * @Date: 2022/3/25 12:45 下午
 */
public class Origin2AssetScan extends RichMapFunction<JSONObject, AssetScanTask> {
    private static Logger LOG = Logger.getLogger(Origin2AssetScan.class);

    private String locationPath;
    private IpLocation ipLocation;

    public Origin2AssetScan(String locationPath) {
        this.locationPath = locationPath;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ipLocation = new IpLocation(locationPath);
    }

    @Override
    public AssetScanTask map(JSONObject o) {
        try {
            //todo  更换实体类
            String taskId = o.get(ScanCollectConstant.TASK_ID).toString();
            String scanTime = o.get(ScanCollectConstant.SCAN_TIME).toString();
            //resource info
            JSONObject resourceObj = JSON.parseObject(o.get(ScanCollectConstant.RESOURCE_INFO).toString());


            if (resourceObj.size() > 0) {
                //deviceIpAddress
                String deviceIpAddress = resourceObj.get(ScanCollectConstant.DEVICE_IP_ADDRESS).toString();
                String deviceMac = resourceObj.get(ScanCollectConstant.DEVICE_MAC).toString();
                String ipAddressOwnership = resourceObj.get(ScanCollectConstant.IP_ADDRESS_OWNERSHIP).toString();
                String deviceType = resourceObj.get(ScanCollectConstant.DEVICE_TYPE).toString();
                String osInfo = resourceObj.get(ScanCollectConstant.OS_INFO).toString();
                String systemFingerprintInfo = resourceObj.get(ScanCollectConstant.SYSTEM_FINGERPRINT_INFO).toString();
                //openServiceOfPort [obj]
                String openServiceOfPortJson = resourceObj.get(ScanCollectConstant.OPEN_SERVICE_OF_PORT).toString();
                List<OpenServiceOfPort> openServiceOfPorts = JSON.parseArray(openServiceOfPortJson, OpenServiceOfPort.class);
                //[obj]
                String messageOrientedMiddlewareJson = resourceObj.get(ScanCollectConstant.MESSAGE_ORIENTED_MIDDLEWARE).toString();
                List<MessageOrientedMiddlewareScan> messageOrientedMiddlewares = JSON.parseArray(messageOrientedMiddlewareJson, MessageOrientedMiddlewareScan.class);
                //[obj]
                String dataBaseInfoJson = resourceObj.get(ScanCollectConstant.DATA_BASE_INFO).toString();
                List<DataBaseInfoScan> dataBaseInfos = JSON.parseArray(dataBaseInfoJson, DataBaseInfoScan.class);

                String province = "";
                //ip归属省份
                try {
                    Location location = ipLocation.fetchIPLocation(deviceIpAddress.trim());
                    province = location.country;
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
                /**
                 * asset_id varchar(255) COMMENT 'ip+mac确定资产唯一',
                 *      *     task_id varchar(255) COMMENT '任务ID',
                 *      *     scan_time datetime COMMENT '扫描时间',
                 *      *     device_name varchar(64) COMMENT '资产名称',````````
                 *      *     device_ip varchar(64) COMMENT '资产IP',
                 *      *     device_ip_ownership varchar(255) DEFAULT "" COMMENT 'ip归属地',
                 *      *     device_mac varchar(256) COMMENT '资产MAC地址',
                 *      *     device_type varchar(64) COMMENT '资产类型，ID与label表关联',````````
                 *      *     data_base_info varchar(65532) COMMENT '数据库信息',
                 *      *     kernel_version varchar(255) COMMENT '内核版本号',
                 *      *     message_oriented_middleware varchar(65532) COMMENT '消息中间件',
                 *      *     open_service_of_port varchar(65532) COMMENT '服务信息及端口号',
                 *      *     os_info varchar(65532) COMMENT '操作系统信息',
                 *      *     patch_properties varchar(65532) COMMENT '补丁属性',
                 *      *     program_info varchar(65532) COMMENT '进程信息',
                 *      *     system_fingerprint_info varchar(65532) COMMENT '系统指纹信息',
                 *      *     label_id varchar(255) DEFAULT "" COMMENT '标签ID',
                 *      *     label_type1 varchar(255) DEFAULT "" COMMENT'一级标签',```````
                 *      *     label_type2 varchar(255) DEFAULT "" COMMENT'二级标签'````````
                 */
                AssetScanTask scanTask = new AssetScanTask();
                scanTask.setAsset_id(deviceIpAddress + "_" + deviceMac);
                scanTask.setTask_id(taskId);
                scanTask.setScan_time(scanTime);
                scanTask.setDevice_ip(deviceIpAddress);
                scanTask.setDevice_ip_ownership(province);
                scanTask.setDevice_mac(deviceMac);
                scanTask.setData_base_info(dataBaseInfoJson);
                scanTask.setMessage_oriented_middleware(messageOrientedMiddlewareJson);
                scanTask.setOpen_service_of_port(openServiceOfPortJson);
                scanTask.setOs_info(osInfo);

/*
                AssetScanOrigin scan = new AssetScanOrigin();
                scan.setTaskID(taskId);
                scan.setScanTime(scanTime);
                scan.setDeviceIPAddress(deviceIpAddress);
                scan.setiPAddressOwnership("".equals(province) ? "中国" : province);
                scan.setDeviceType(deviceType);
                scan.setoSInfo(osInfo);
                scan.setSystemFingerprintInfo(systemFingerprintInfo);
                scan.setOpenServiceOfPort(openServiceOfPorts);
                scan.setMessageOrientedMiddleware(messageOrientedMiddlewares);
                scan.setDataBaseInfos(dataBaseInfos);*/
                return scanTask;
            }
        } catch (Exception e) {
            LOG.error("资产原始数据转AssetScanOrigin对象出错：" + e.getMessage(), e);
        }
        return null;
    }
}
