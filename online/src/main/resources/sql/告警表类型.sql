CREATE TABLE IF NOT EXISTS sdc.event_alarm_type(
    type_id varchar(64) COMMENT '事件类型ID',
    event_type_name varchar(64) COMMENT '事件类型名称',
    event_type_switch varchar(64) COMMENT ''
)
DISTRIBUTED BY HASH(type_id) BUCKETS 1;

