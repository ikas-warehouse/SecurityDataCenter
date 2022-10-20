CREATE TABLE IF NOT EXISTS sdc.event_origin_log(
    id varchar(64) COMMENT 'UUID',
    content varchar(65533) COMMENT '原始内容',
    update_time datetime COMMENT '修改时间'
)
PARTITION BY RANGE(update_time)()
DISTRIBUTED BY HASH(id) BUCKETS 8
properties(
  "dynamic_partition.enable" = "true",
  "dynamic_partition.time_unit" = "DAY",
  "dynamic_partition.create_history_partition" = "true",
  "dynamic_partition.start" = "-180",
  "dynamic_partition.end" = "1",
  "dynamic_partition.prefix" = "p",
  "dynamic_partition.buckets" = "8"
);

