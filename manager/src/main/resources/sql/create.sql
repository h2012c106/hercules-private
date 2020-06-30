create database `hercules`;

create table `hercules`.`cluster` (
    `id` int(8) NOT NULL AUTO_INCREMENT COMMENT 'id',
    `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '集群名称',
    `desc` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '集群描述',
    `cloud` varchar(16) NOT NULL COMMENT '云: TX-腾讯;HW-华为;',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `NAME_KEY` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='集群信息表';

create table `hercules`.`node` (
    `id` int(8) NOT NULL AUTO_INCREMENT COMMENT 'id',
    `cluster_id` int(8) NOT NULL COMMENT '集群id',
    `host` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '节点host',
    `port` int(8) NOT NULL DEFAULT 2964 COMMENT '节点通信port',
    `status` varchar(16) NOT NULL DEFAULT 'ON' COMMENT '节点状态: OFF-关闭;ON-开启;',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `NODE_ADDRESS` (`host`, `port`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='节点信息表';

create table `hercules`.`job` (
    `id` int(8) NOT NULL AUTO_INCREMENT COMMENT 'id',
    `name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '任务名称',
    `desc` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL COMMENT '任务描述',
    `source` varchar(64) NOT NULL COMMENT '源数据源名称',
    `target` varchar(64) NOT NULL COMMENT '目标数据源名称',
    `source_param` json NOT NULL COMMENT '源数据源参数',
    `target_param` json NOT NULL COMMENT '目标数据源参数',
    `common_param` json NOT NULL COMMENT 'common参数',
    `D_param` json DEFAULT NULL COMMENT '-D参数',
    `source_template_param` json NOT NULL COMMENT '源数据源带模版的参数',
    `target_template_param` json NOT NULL COMMENT '目标数据源带模版的参数',
    `common_template_param` json NOT NULL COMMENT 'common带模版的参数',
    `D_template_param` json NOT NULL COMMENT '-D带模版的参数',
    `running_task_id` int(8) DEFAULT NULL COMMENT '标志正在起着哪个task，兼控制job不可同时起超过一个task',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `NAME_KEY` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='任务信息表';

create table `hercules`.`task` (
    `id` int(8) NOT NULL AUTO_INCREMENT COMMENT 'id',
    `job_id` int(8) NOT NULL COMMENT '任务id',
    `node_id` int(8) NOT NULL COMMENT '执行节点id',
    `command` varchar(1024) NOT NULL COMMENT '作业语句',
    `submit_time` timestamp NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '作业开始时间',
    `start_time` timestamp NOT NULL DEFAULT '1970-01-01 08:00:01' COMMENT '作业开始时间',
    `total_time_ms` bigint(20) NOT NULL DEFAULT 0 COMMENT '作业总用时',
    `mr_time_ms` bigint(20) DEFAULT NULL COMMENT '作业MR环节用时',
    `map_num` int(8) DEFAULT NULL COMMENT 'map数目',
    `record_num` bigint(20) DEFAULT NULL COMMENT '转换行数',
    `estimated_byte_size` bigint(32) DEFAULT NULL COMMENT '估算转换byte大小',
    `status` varchar(16) NOT NULL DEFAULT 'PENDING' COMMENT '作业状态: PENDING;RUNNING;KILLED;SUCCESS;ERROR;DONE_UNKNOWN;ZOMBIE;',
    `application_id` varchar(64) DEFAULT NULL COMMENT 'application id',
    `mr_log_url` varchar(256) DEFAULT NULL COMMENT 'map日志url',
    `mr_progress` tinyint(1) DEFAULT 0 COMMENT 'mr进度',
    `caller` varchar(16) NOT NULL COMMENT '调用方(决定调用结束行为): RDS;AIRFLOW;',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    PRIMARY KEY (`id`),
    KEY `create_time_key` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='作业信息表';
