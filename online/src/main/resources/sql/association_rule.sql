CREATE TABLE sdc.association_rule (
    `id` varchar(255) DEFAULT NULL COMMENT '规则ID' ,
    `strategy_name` varchar(255) DEFAULT NULL COMMENT '策略名称',
    `strategy_rule` varchar(255) DEFAULT NULL COMMENT '策略规则',
    `key_by` varchar(255) DEFAULT NULL COMMENT '分组字段',
    `is_roll` varchar(255) DEFAULT NULL COMMENT '是否为滚动窗口',
    `window_time_start` varchar(255) DEFAULT NULL COMMENT '窗口开始时间',
    `window_time_end` varchar(255) DEFAULT NULL COMMENT '窗口结束时间',
    `trigger_condition` varchar(255) DEFAULT NULL COMMENT '触发条件',
    `job_id` varchar(255) DEFAULT NULL COMMENT '回填JOB的进程ID',
    `gen_time` varchar(30) DEFAULT NULL COMMENT '策略生成时间',
    `sink_result` varchar(2048) COMMENT '输出规则',
    `rule_describe` varchar(255) DEFAULT NULL COMMENT '规则描述',
    `is_single` varchar(1) DEFAULT NULL COMMENT '是否为单事件',
    `pattern` varchar(2048) COMMENT '样式',
    `cluster_analysis` varchar(1) DEFAULT NULL COMMENT '是否需要聚合分析 1：是 0：否',
    `generalize_again` varchar(10) DEFAULT '0' COMMENT '是否需要二次泛化',
    `mul_analysis_group_enable` varchar(10) DEFAULT '0' COMMENT '二次泛化时是否需要默认分组',
    `rule_state` varchar(11) DEFAULT NULL COMMENT '规则状态'
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1;