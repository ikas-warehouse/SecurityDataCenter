CREATE TABLE sdc.association_rule (
    `id` varchar(255) DEFAULT NULL,
    `strategy_name` varchar(255) DEFAULT NULL,
    `strategy_rule` varchar(255) DEFAULT NULL,
    `key_by` varchar(255) DEFAULT NULL,
    `is_roll` varchar(255) DEFAULT NULL,
    `window_time_start` varchar(255) DEFAULT NULL,
    `window_time_end` varchar(255) DEFAULT NULL,
    `trigger_condition` varchar(255) DEFAULT NULL,
    `job_id` varchar(255) DEFAULT NULL,
    `gen_time` varchar(30) DEFAULT NULL,
    `sink_result` varchar(2048),
    `rule_describe` varchar(255) DEFAULT NULL,
    `is_single` varchar(1) DEFAULT NULL,
    `pattern` varchar(2048),
    `cluster_analysis` int(11) DEFAULT NULL,
    `generalize_again` varchar(10) DEFAULT '0',
    `mul_anaLysis_group_enable` varchar(10) DEFAULT '0',
    `rule_state` varchar(11) DEFAULT NULL
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1;