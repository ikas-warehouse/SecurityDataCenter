package brd.asset.entity;

import lombok.Data;

/**
 * @program SecurityDataCenter
 * @description: 资产扫描任务原始数据对象
 * @author: 蒋青松
 * @create: 2022/08/16 11:25
 */
@Data
public class AssetScanTask {
    /**
     * asset_id varchar(255) COMMENT 'ip+mac确定资产唯一',
     *     task_id varchar(255) COMMENT '任务ID',
     *     scan_time datetime COMMENT '扫描时间',
     *     device_name varchar(64) COMMENT '资产名称',
     *     device_ip varchar(64) COMMENT '资产IP',
     *     device_ip_ownership varchar(255) DEFAULT "" COMMENT 'ip归属地',
     *     device_mac varchar(256) COMMENT '资产MAC地址',
     *     device_type varchar(64) COMMENT '资产类型，ID与label表关联',
     *     data_base_info varchar(65532) COMMENT '数据库信息',
     *     kernel_version varchar(255) COMMENT '内核版本号',
     *     message_oriented_middleware varchar(65532) COMMENT '消息中间件',
     *     open_service_of_port varchar(65532) COMMENT '服务信息及端口号',
     *     os_info varchar(65532) COMMENT '操作系统信息',
     *     patch_properties varchar(65532) COMMENT '补丁属性',
     *     program_info varchar(65532) COMMENT '进程信息',
     *     system_fingerprint_info varchar(65532) COMMENT '系统指纹信息',
     *     label_id varchar(255) DEFAULT "" COMMENT '标签ID',
     *     label_type1 varchar(255) DEFAULT "" COMMENT'一级标签',
     *     label_type2 varchar(255) DEFAULT "" COMMENT'二级标签'
     */
    public String asset_id;
    public String task_id;
    public String scan_time;
    public String device_name;
    public String device_ip;
    public String device_ip_ownership;
    public String device_mac;
    public String device_type;
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

    public AssetScanTask() {
    }
}
