CREATE DATABASE IF NOT EXISTS bigdata_dataservice DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
SET FOREIGN_KEY_CHECKS = 0;
-- ----------------------------
-- Table structure for Column_alias
-- ----------------------------
DROP TABLE IF EXISTS `Column_alias`;
CREATE TABLE `Column_alias`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) NOT NULL,
  `column_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `column_alias` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `state` int(11) NOT NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `des` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `metric` bit(1) NULL DEFAULT b'0' COMMENT '0:false;1:true',
  `is_delete` int(11) NOT NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  `data_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for Database_info
-- ----------------------------
DROP TABLE IF EXISTS `Database_info`;
CREATE TABLE `Database_info`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `db_id` int(11) NOT NULL,
  `database` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `service_path` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `state` int(11) NOT NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_delete` int(11) NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for Db_info
-- ----------------------------
DROP TABLE IF EXISTS `Db_info`;
CREATE TABLE `Db_info`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `db_url` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `db_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `userName` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `password` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `service_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `service_path` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `state` int(11) NOT NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `common_service` smallint(1) NOT NULL COMMENT '1是通用服务 0是非通用服务',
  `extend` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_delete` int(11) NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for Quoto_info
-- ----------------------------
DROP TABLE IF EXISTS `Quoto_info`;
CREATE TABLE `Quoto_info`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table_id` int(11) NOT NULL,
  `quoto_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `quoto_sql` varchar(600) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `state` int(11) NOT NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_delete` int(11) NOT NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for Table_info
-- ----------------------------
DROP TABLE IF EXISTS `Table_info`;
CREATE TABLE `Table_info`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `database_id` int(11) NOT NULL,
  `table_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `theme_id` int(11) NULL DEFAULT NULL,
  `state` int(11) NOT NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_delete` int(11) NOT NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  `table_alias` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `theme_id`(`theme_id`) USING BTREE,
  CONSTRAINT `Table_info_ibfk_1` FOREIGN KEY (`theme_id`) REFERENCES `theme` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for adjective
-- ----------------------------
DROP TABLE IF EXISTS `adjective`;
CREATE TABLE `adjective`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dimension_id` int(11) NULL DEFAULT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `column_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `type` int(11) NULL DEFAULT 2 COMMENT '1:时间修饰词,2业务修饰词',
  `req_parm_type` int(11) NULL DEFAULT 2 COMMENT '1:变量 2:常量',
  `req_parm` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `fields` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;1:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `code`(`code`, `deleted`) USING BTREE,
  INDEX `dimension_id`(`dimension_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for api_doc
-- ----------------------------
DROP TABLE IF EXISTS `api_doc`;
CREATE TABLE `api_doc`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '接口名称',
  `service_path` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '服务地址',
  `method` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'get' COMMENT '请求方式',
  `parameters` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `example_req` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `example_response` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `state` int(11) NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_delete` int(11) NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for article
-- ----------------------------
DROP TABLE IF EXISTS `article`;
CREATE TABLE `article`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `content` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for business
-- ----------------------------
DROP TABLE IF EXISTS `business`;
CREATE TABLE `business`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `pid` int(11) NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for business_process
-- ----------------------------
DROP TABLE IF EXISTS `business_process`;
CREATE TABLE `business_process`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `theme_id` int(11) NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `theme_id`(`theme_id`) USING BTREE,
  CONSTRAINT `business_process_ibfk_1` FOREIGN KEY (`theme_id`) REFERENCES `theme` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for cycle
-- ----------------------------
DROP TABLE IF EXISTS `cycle`;
CREATE TABLE `cycle`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for data_set
-- ----------------------------
DROP TABLE IF EXISTS `data_set`;
CREATE TABLE `data_set`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `data_type` tinyint(1) NOT NULL COMMENT '1:sql 2:标签 3:csv',
  `data_source_id` int(11) NULL DEFAULT NULL,
  `data_source_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `data_source_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `data_connect_type` tinyint(1) NOT NULL DEFAULT 1 COMMENT '1:直连 2:抽取',
  `directory_id` int(11) NULL DEFAULT NULL COMMENT '目录层级',
  `data_rule` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `data_columns` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `tag_item_complexs` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `tag_enum_values` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `mapping_ck_table` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  `state` tinyint(4) NOT NULL DEFAULT 2 COMMENT '1：运行中 2：完成 3:失败',
  `message` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for data_set_task
-- ----------------------------
DROP TABLE IF EXISTS `data_set_task`;
CREATE TABLE `data_set_task`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `data_set_id` int(11) NOT NULL,
  `type` smallint(1) NOT NULL COMMENT '1 手动 2调度',
  `import_type` smallint(1) NOT NULL,
  `export_type` smallint(1) NOT NULL DEFAULT 1 COMMENT '1 默认导出ck 2s3',
  `export_parameters` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '导出类型特有的参数信息',
  `sync_type` smallint(1) NULL DEFAULT NULL,
  `condition` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `cron` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `advanced_parameters` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `oozie_hue_uuid` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `workflow_hue_uuid` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `waterdrop_hue_uuid` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `run_info` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `state` smallint(1) NULL DEFAULT 0 COMMENT '0:初始化 1:运行中 2:成功 3:失败',
  `start_time` timestamp(0) NULL DEFAULT NULL,
  `end_time` timestamp(0) NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dataservice_access_history
-- ----------------------------
DROP TABLE IF EXISTS `dataservice_access_history`;
CREATE TABLE `dataservice_access_history`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `token` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `service_path` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `table_alias` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `execute_sql` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `success` bit(1) NOT NULL DEFAULT b'1' COMMENT '0 false 1success',
  `msg` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dimension
-- ----------------------------
DROP TABLE IF EXISTS `dimension`;
CREATE TABLE `dimension`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dimension_object_id` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `value_type` int(255) NULL DEFAULT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for dimension_object
-- ----------------------------
DROP TABLE IF EXISTS `dimension_object`;
CREATE TABLE `dimension_object`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `type` int(1) NOT NULL DEFAULT 1 COMMENT '1:全局维度,2:私有维度',
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for directory
-- ----------------------------
DROP TABLE IF EXISTS `directory`;
CREATE TABLE `directory`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `pid` int(11) NOT NULL DEFAULT 0,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `name`(`name`, `pid`, `creator`, `deleted`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for metadata_source
-- ----------------------------
DROP TABLE IF EXISTS `metadata_source`;
CREATE TABLE `metadata_source`  (
  `table_type_id` int(2) NULL DEFAULT NULL,
  `datahub_instance` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `datahub_ingestion_source` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted'
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
-- ----------------------------
-- Table structure for metadata_table
-- ----------------------------
DROP TABLE IF EXISTS `metadata_table`;
CREATE TABLE `metadata_table`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `database_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `table_type` int(11) NOT NULL COMMENT '1:hive表 2:clickhouse表',
  `storage_format` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `external_table` bit(1) NULL DEFAULT b'1' COMMENT '0:false;1:true',
  `system_storage_location` bit(1) NULL DEFAULT b'1' COMMENT '0:false;1:true',
  `storage_location` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `system_delimiter` bit(1) NULL DEFAULT b'1' COMMENT '0:false;1:true',
  `delimiter` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `partition` bit(1) NULL DEFAULT b'1' COMMENT '0:false;1:true',
  `model_level` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `life_cycle` int(5) NULL DEFAULT NULL,
  `order_field` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT 'clickhouse表排序的字段',
  `data_domain` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `theme_id` int(11) NOT NULL,
  `ddl` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `columns` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `partition_field` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(2000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  `mapping_instance_table` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'ck对应的实力表',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `data_domain_id`(`data_domain`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for quoto
-- ----------------------------
DROP TABLE IF EXISTS `quoto`;
CREATE TABLE `quoto`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `field` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `metric` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `business_process_id` int(11) NULL DEFAULT NULL,
  `theme_id` int(11) NOT NULL,
  `quoto_level` int(11) NOT NULL,
  `data_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `data_unit` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `table_id` int(11) NULL DEFAULT NULL,
  `time_column_id` int(11) NULL DEFAULT NULL,
  `sql` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `use_sql` bit(1) NULL DEFAULT b'0' COMMENT '0:false;1:true',
  `accumulation` bit(1) NOT NULL,
  `dimension` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `type` tinyint(4) NOT NULL,
  `origin_quoto` int(11) NULL DEFAULT NULL,
  `adjective` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `cycle` int(11) NULL DEFAULT NULL,
  `expression` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `quotos` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '复合关联的指标',
  `state` tinyint(4) NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `name`(`name`, `deleted`) USING BTREE,
  UNIQUE INDEX `field`(`field`, `deleted`) USING BTREE,
  INDEX `table_id`(`table_id`) USING BTREE,
  INDEX `theme_id`(`theme_id`) USING BTREE,
  CONSTRAINT `quoto_ibfk_2` FOREIGN KEY (`theme_id`) REFERENCES `theme` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for quoto_access_history
-- ----------------------------
DROP TABLE IF EXISTS `quoto_access_history`;
CREATE TABLE `quoto_access_history`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `quoto_id` int(11) NOT NULL,
  `quoto_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `business` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `theme` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `level` int(11) NULL DEFAULT NULL,
  `type` int(11) NOT NULL,
  `success` bit(1) NULL DEFAULT b'1' COMMENT '0:false;1:true',
  `message` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `quoto_id`(`quoto_id`) USING BTREE,
  CONSTRAINT `quoto_access_history_ibfk_1` FOREIGN KEY (`quoto_id`) REFERENCES `quoto` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for quoto_tag
-- ----------------------------
DROP TABLE IF EXISTS `quoto_tag`;
CREATE TABLE `quoto_tag`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tag_id` int(11) NOT NULL,
  `quoto_id` int(11) NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `tag_id`(`tag_id`, `quoto_id`, `deleted`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for quoto_update_history
-- ----------------------------
DROP TABLE IF EXISTS `quoto_update_history`;
CREATE TABLE `quoto_update_history`  (
  `id` int(11) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `field` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `metric` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `business_process_id` int(11) NULL DEFAULT NULL,
  `theme_id` int(11) NOT NULL,
  `quoto_level` int(11) NOT NULL,
  `data_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `data_unit` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `table_id` int(11) NULL DEFAULT NULL,
  `time_column_id` int(11) NULL DEFAULT NULL,
  `sql` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `use_sql` bit(1) NULL DEFAULT b'0' COMMENT '0:false;1:true',
  `accumulation` bit(1) NOT NULL,
  `dimension` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '',
  `type` tinyint(4) NOT NULL,
  `origin_quoto` int(11) NULL DEFAULT NULL,
  `adjective` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `cycle` int(11) NULL DEFAULT NULL,
  `expression` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `quotos` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '复合关联的指标',
  `state` tinyint(4) NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT NULL,
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted'
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for tag
-- ----------------------------
DROP TABLE IF EXISTS `tag`;
CREATE TABLE `tag`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `color` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for theme
-- ----------------------------
DROP TABLE IF EXISTS `theme`;
CREATE TABLE `theme`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `en_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `business_id` int(11) NOT NULL,
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `descr` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `deleted` bit(1) NULL DEFAULT b'0' COMMENT '0:false-not deleted;null:true-deleted',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `business_id`(`business_id`) USING BTREE,
  CONSTRAINT `theme_ibfk_1` FOREIGN KEY (`business_id`) REFERENCES `business` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for user_token
-- ----------------------------
DROP TABLE IF EXISTS `user_token`;
CREATE TABLE `user_token`  (
  `id` int(5) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `token` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tables` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '可以访问的表',
  `creator` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `des` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  `state` int(11) NULL DEFAULT 1 COMMENT '1是可用 0是不可用',
  `is_delete` int(1) NULL DEFAULT 0 COMMENT '1 删除 0未删除',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;