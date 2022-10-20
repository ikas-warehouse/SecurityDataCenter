CREATE TABLE IF NOT EXISTS sdc.alarm_to_format(
    alarm_id varchar(64) COMMENT '告警ID',
    format_id varchar(64) COMMENT '泛化表日志ID',
    update_time datetime COMMENT '修改时间'
)
PARTITION BY RANGE(update_time)()
DISTRIBUTED BY HASH(alarm_id) BUCKETS 8
properties(
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.create_history_partition" = "true",
  "dynamic_partition.start" = "-180",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "8"
);

