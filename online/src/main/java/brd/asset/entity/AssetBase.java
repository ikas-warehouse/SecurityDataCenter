package brd.asset.entity;

import lombok.Data;

/**
 * @Author: leo.j
 * @desc: 资产基础表
 * @Date: 2022/4/1 9:56 上午
 */
@Data
public class AssetBase {
    public String asset_id;
    public String device_name;
    public String device_ip;
    public String device_ip_ownership;
    public String device_mac;
    public String device_type;
    public String physical_address;
    public String machine_room;
    public String machine_box;
    public String manufacturers;
    public String sn;
    public String model;
    public String business_system;
    public String responsible_person;
    public String responsible_telephone;
    public String asset_desc;
    public String confidentiality;
    public String integrality;
    public String usability;
    public String importance;
    public String permitted;
    public String longitude;
    public String latitude;
    public String responsible_unit;
    public String responsible_department;
    public String responsible_group;
    public String data_base_info;
    public String kernel_version;
    public String message_oriented_middleware;
    public String open_service_of_port;
    public String os_info;
    public String patch_properties;
    public String program_info;
    public String system_fingerprint_info;
    public String label_id;
    public String label_type1;
    public String label_type2;
    public String update_time;

    public AssetBase() {
    }


}
