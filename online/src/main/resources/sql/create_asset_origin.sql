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
    properties(
                  "dynamic_partition.enable" = "true",
                  "dynamic_partition.time_unit" = "DAY",
                  "dynamic_partition.create_history_partition" = "true",
                  "dynamic_partition.start" = "-30",
                  "dynamic_partition.end" = "2",
                  "dynamic_partition.prefix" = "p",
                  "dynamic_partition.buckets" = "8"
              );