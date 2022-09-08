package brd.asset.flink.fun;

import brd.asset.entity.AssetBase;
import brd.asset.entity.AssetScanTask;
import brd.common.TimeUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @program SecurityDataCenter
 * @description: AssetScanTask转AssetBase
 * @author: 蒋青松
 * @create: 2022/09/08 15:20
 */
public class AssetScanTask2AssetBaseMap implements MapFunction<AssetScanTask, AssetBase> {
    @Override
    public AssetBase map(AssetScanTask assetScanTask) throws Exception {

        String assetId = assetScanTask.getAsset_id();
        String deviceName = assetScanTask.getDevice_name();
        String deviceIp = assetScanTask.getDevice_ip();
        String deviceIpOwnership = assetScanTask.getDevice_ip_ownership();
        String deviceMac = assetScanTask.getDevice_mac();
        String deviceType = assetScanTask.getDevice_type();
        String dataBaseInfo = assetScanTask.getData_base_info();
        String kernelVersion = assetScanTask.getKernel_version();
        String messageOrientedMiddleware = assetScanTask.getMessage_oriented_middleware();
        String openServiceOfPort = assetScanTask.getOpen_service_of_port();
        String osInfo = assetScanTask.getOs_info();
        String patchProperties = assetScanTask.getPatch_properties();
        String programInfo = assetScanTask.getProgram_info();
        String systemFingerprintInfo = assetScanTask.getSystem_fingerprint_info();
        String labelId = assetScanTask.getLabel_id();
        String labelType1 = assetScanTask.getLabel_type1();
        String labelType2 = assetScanTask.getLabel_type2();

        AssetBase assetBase = new AssetBase();
        assetBase.setAsset_id(assetId);
        assetBase.setDevice_name(deviceName);
        assetBase.setDevice_ip(deviceIp);
        assetBase.setDevice_ip_ownership(deviceIpOwnership);
        assetBase.setDevice_mac(deviceMac);
        assetBase.setDevice_type(deviceType);
        assetBase.setData_base_info(dataBaseInfo);
        assetBase.setKernel_version(kernelVersion);
        assetBase.setMessage_oriented_middleware(messageOrientedMiddleware);
        assetBase.setOpen_service_of_port(openServiceOfPort);
        assetBase.setOs_info(osInfo);
        assetBase.setPatch_properties(patchProperties);
        assetBase.setProgram_info(programInfo);
        assetBase.setSystem_fingerprint_info(systemFingerprintInfo);
        assetBase.setLabel_id(labelId);
        assetBase.setLabel_type1(labelType1);
        assetBase.setLabel_type2(labelType2);
        assetBase.setUpdate_time(TimeUtils.getNow("yyyy-MM-dd HH:mm:ss"));

        return assetBase;
    }
}
