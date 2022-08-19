CREATE TABLE IF NOT EXISTS asset_base(
    `asset_id` varchar(64) DEFAULT "" COMMENT '资产ID',
    `data_base_info` text COMMENT '数据库信息',
    `device_ip_address` varchar(255) DEFAULT "" COMMENT '设备IP地址',
    `device_name` varchar(255) DEFAULT "" COMMENT '设备名称',
    `device_type` varchar(255) DEFAULT "" COMMENT '设备类型',
    `ip_address_ownership` varchar(255) DEFAULT "" COMMENT 'ip归属地',
    `kernel_version` text COMMENT '内核版本号',
    `label_id` varchar(255) DEFAULT "",
    `label_type1` varchar(255) DEFAULT "",
    `label_type2` varchar(255) DEFAULT "",
    `message_oriented_middleware` varchar(65532) COMMENT '消息中间件',
    `open_service_of_port` varchar(65532) COMMENT '服务信息及端口号',
    `os_info` varchar(65532) COMMENT '操作系统信息',
    `patch_properties` varchar(65532) COMMENT '补丁属性',
    `program_info` varchar(65532) COMMENT '进程信息',
    `resource_name` varchar(255) DEFAULT "" ,
    `scan_time` datetime COMMENT '扫描时间',
    `system_fingerprint_info` varchar(65532) COMMENT '系统指纹信息',
    `task_id` varchar(255) DEFAULT "" COMMENT '任务ID'
    )
    PARTITION BY RANGE(scan_time)()
    DISTRIBUTED BY HASH(device_ip_address)
   ;

CREATE TABLE IF NOT EXISTS sdc.asset_base
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `phone` LARGEINT COMMENT "用户电话",
    `address` VARCHAR(500) COMMENT "用户地址",
    `register_time` DATETIME COMMENT "用户注册时间"
    )
    UNIQUE KEY(ip_address,mac)
    DISTRIBUTED BY HASH(`ip_address`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 3"
               );