package brd.asset.entity;

import lombok.Data;

/**
 * @Author: leo.j
 * @desc: 资产基础表
 * @Date: 2022/4/1 9:56 上午
 */
@Data
public class AssetBase {
    public String asset_id;//资产ID
    public String name;//资产名称
    public String ip_address;//资产IP
    public String mac;//MAC地址
    public String type;//资产类型
    public String os_info;//资产系统信息
    public String physical_address;//物理地址
    public String attribution_group;//
    public String collection_time;//
    public String engine_room;//
    public String engine_box;//
    public String manufacturers;//
    public String sn;//
    public String model;//
    public String business_system;//
    public String telephone;//
    public String confidentiality;//保密性
    public String integrality;//完整性
    public String usability;//可用性
    public String importance;//重要性
    public String permitted;//是否纳管
    public String longitude;//
    public String latitude;//
    public String responsible_person;//责任人
    public String responsible_unit;//责任人
    public String responsible_department;//
    public String responsible_group;//
    public String asset_desc;//

    public AssetBase() {
    }


}
