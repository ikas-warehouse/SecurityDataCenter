CREATE TABLE sdc.label (
     `id` int(11) COMMENT '资产标签表id',
     `label_1` varchar(64) DEFAULT NULL COMMENT '一级标签',
     `label_2` varchar(64) DEFAULT NULL COMMENT '二级标签',
     `label_3` varchar(64) DEFAULT NULL COMMENT '三级标签',
     `slabel_4` varchar(64) DEFAULT NULL COMMENT '四级标签'
) distributed by hash(id) BUCKETS 1;