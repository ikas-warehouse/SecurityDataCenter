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
                AssetScanTask scanTask = new AssetScanTask();
                scanTask.setAsset_id(deviceIpAddress + "_" + deviceMac);
                scanTask.setTask_id(taskId);
                scanTask.setScan_time(scanTime);
                scanTask.setDevice_ip(deviceIpAddress);
                scanTask.setDevice_ip_ownership(ipAddressOwnership);
                scanTask.setDevice_mac(deviceMac);
                scanTask.setDevice_type(deviceType);
                scanTask.setSystem_fingerprint_info(systemFingerprintInfo);
                scanTask.setData_base_info(dataBaseInfoJson);
                scanTask.setMessage_oriented_middleware(messageOrientedMiddlewareJson);
                scanTask.setOpen_service_of_port(openServiceOfPortJson);
                scanTask.setOs_info(osInfo);

                return scanTask;
            }
        } catch (Exception e) {
            LOG.error("资产原始数据转AssetScanOrigin对象出错：" + e.getMessage(), e);
        }
        return null;
    }
}
