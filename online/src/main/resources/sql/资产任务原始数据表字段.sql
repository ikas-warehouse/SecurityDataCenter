CREATE TABLE IF NOT EXISTS sdc.asset_task_origin(
    asset_id varchar(255) COMMENT 'ip+mac确定资产唯一',
    task_id varchar(255) COMMENT '任务ID',
    scan_time datetime COMMENT '扫描时间',
    device_name varchar(64) COMMENT '资产名称',
    device_ip varchar(64) COMMENT '资产IP',
    device_ip_ownership varchar(255) DEFAULT "" COMMENT 'ip归属地',
    device_mac varchar(256) COMMENT '资产MAC地址',
    device_type varchar(64) COMMENT '资产类型，ID与label表关联',
    data_base_info varchar(65532) COMMENT '数据库信息',
    kernel_version varchar(255) COMMENT '内核版本号',
    message_oriented_middleware varchar(65532) COMMENT '消息中间件',
    open_service_of_port varchar(65532) COMMENT '服务信息及端口号',
    os_info varchar(65532) COMMENT '操作系统信息',
    patch_properties varchar(65532) COMMENT '补丁属性',
    program_info varchar(65532) COMMENT '进程信息',
    system_fingerprint_info varchar(65532) COMMENT '系统指纹信息',
    label_id varchar(255) DEFAULT "" COMMENT '标签ID',
    label_type1 varchar(255) DEFAULT "" COMMENT'一级标签',
    label_type2 varchar(255) DEFAULT "" COMMENT'二级标签'
)PARTITION BY RANGE(scan_time)()
DISTRIBUTED BY HASH(device_ip)
properties(
              "dynamic_partition.enable" = "true",
              "dynamic_partition.time_unit" = "DAY",
              "dynamic_partition.create_history_partition" = "true",
              "dynamic_partition.start" = "-30",
              "dynamic_partition.end" = "2",
              "dynamic_partition.prefix" = "p",
              "dynamic_partition.buckets" = "8"
          );