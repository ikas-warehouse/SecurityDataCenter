package brd.asset.pojo;

import java.util.List;

/**
 * @author leo.J
 * @description 远程扫描采集原始数据
 * @date 2022-08-09 13:48
 */
public class AssetScanOrigin {
    private String assetId;
    private String taskID;
    private String scanTime;
    private String deviceIPAddress;
    private String iPAddressOwnership;
    private String deviceType;
    private String oSInfo;
    private String systemFingerprintInfo;
    private String labelIds;
    private List<OpenServiceOfPort> openServiceOfPort;
    private List<MessageOrientedMiddlewareScan> messageOrientedMiddleware;
    private List<DataBaseInfoScan> dataBaseInfos;


    public AssetScanOrigin() {
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getLabelIds() {
        return labelIds;
    }

    public void setLabelIds(String labelIds) {
        this.labelIds = labelIds;
    }

    public String getTaskID() {
        return taskID;
    }

    public void setTaskID(String taskID) {
        this.taskID = taskID;
    }

    public String getScanTime() {
        return scanTime;
    }

    public void setScanTime(String scanTime) {
        this.scanTime = scanTime;
    }

    public String getDeviceIPAddress() {
        return deviceIPAddress;
    }

    public void setDeviceIPAddress(String deviceIPAddress) {
        this.deviceIPAddress = deviceIPAddress;
    }

    public String getiPAddressOwnership() {
        return iPAddressOwnership;
    }

    public void setiPAddressOwnership(String iPAddressOwnership) {
        this.iPAddressOwnership = iPAddressOwnership;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getoSInfo() {
        return oSInfo;
    }

    public void setoSInfo(String oSInfo) {
        this.oSInfo = oSInfo;
    }

    public String getSystemFingerprintInfo() {
        return systemFingerprintInfo;
    }

    public void setSystemFingerprintInfo(String systemFingerprintInfo) {
        this.systemFingerprintInfo = systemFingerprintInfo;
    }

    public List<OpenServiceOfPort> getOpenServiceOfPort() {
        return openServiceOfPort;
    }

    public void setOpenServiceOfPort(List<OpenServiceOfPort> openServiceOfPort) {
        this.openServiceOfPort = openServiceOfPort;
    }

    public List<MessageOrientedMiddlewareScan> getMessageOrientedMiddleware() {
        return messageOrientedMiddleware;
    }

    public void setMessageOrientedMiddleware(List<MessageOrientedMiddlewareScan> messageOrientedMiddleware) {
        this.messageOrientedMiddleware = messageOrientedMiddleware;
    }

    public List<DataBaseInfoScan> getDataBaseInfos() {
        return dataBaseInfos;
    }

    public void setDataBaseInfos(List<DataBaseInfoScan> dataBaseInfos) {
        this.dataBaseInfos = dataBaseInfos;
    }

    @Override
    public String toString() {
        return "AssetScanOrigin{" +
                ", TaskID='" + taskID + '\'' +
                ", ScanTime='" + scanTime + '\'' +
                ", DeviceIPAddress='" + deviceIPAddress + '\'' +
                ", IPAddressOwnership='" + iPAddressOwnership + '\'' +
                ", DeviceType='" + deviceType + '\'' +
                ", OSInfo='" + oSInfo + '\'' +
                ", SystemFingerprintInfo='" + systemFingerprintInfo + '\'' +
                ", OpenServiceOfPort=" + openServiceOfPort +
                ", MessageOrientedMiddleware=" + messageOrientedMiddleware +
                ", dataBaseInfos=" + dataBaseInfos +
                '}';
    }
}
