CREATE TABLE `label_key` (
     `id` int(11)  COMMENT '标签关键词id',
     `keyword` varchar(64) DEFAULT NULL COMMENT '标签关键词',
     `label_id` int(11) DEFAULT NULL COMMENT '标签表id',
     `remarks` varchar(255) DEFAULT NULL COMMENT '备注'
) distributed by hash(id) BUCKETS 1;