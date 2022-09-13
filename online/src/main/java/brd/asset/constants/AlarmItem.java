package brd.asset.constants;

/**
 * @Author: leo.j
 * @desc: 异常资产告警枚举类
 * @Date: 2021/12/24 2:47 下午
 */
public enum AlarmItem {
    ABNORMAL_OPEN_PORT("679c961630f049ab904e10580cacd118", "开放了超过阈值数量的端口/服务"),
    ABNORMAL_ASSET_CHANGE("aded6ae32888443f97d2ea09ac4275a5", "疑似资产被替换"),
    ABNORMAL_ASSET_DOWN("c95a6d62ef3d43b8995258375fce1d4f", "原有IP消失问题"),
    ABNORMAL_ASSET_NO_GROUP("dcb3fba595cf469b8dd390edcb2d9033", "资产未配置所属单位、责任人"),
    ABNORMAL_ASSET_UNKNOWN("b42e1191dc404e90b100384406dfeb5b", "未在纳管范围内却已经在线联网的资产"),
    ABNORMAL_PROESS("b830bf36fa424099948f03906f91a221", "由后门、木马、病毒等恶意程序引起的端口进程异常变化");
    //事件ID
    private String eventId;
    //事件描述
    private String eventDesc;

    AlarmItem(String eventId, String eventDesc) {
        this.eventId = eventId;
        this.eventDesc = eventDesc;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventDesc() {
        return eventDesc;
    }
}
