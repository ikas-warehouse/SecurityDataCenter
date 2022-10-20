
CREATE TABLE IF NOT EXISTS sdc.tj_threat (
     `type` varchar(32) NULL COMMENT '数据类型',
     `value` varchar(64) NULL COMMENT '值',
     `geo` varchar(255) NULL COMMENT '地理位置（格式：国家^省^市）',
     `reputation` varchar(1024) NULL COMMENT '信誉类集合',
     `in_time` varchar(64) NULL COMMENT '入库时间'
) ENGINE=OLAP
UNIQUE KEY(`type`, `value`)
DISTRIBUTED BY HASH(`type`) BUCKETS 8;

