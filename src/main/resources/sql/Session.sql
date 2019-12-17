-- CREATE TASK

CREATE TABLE `t_Task`(
  `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_name` VARCHAR(200) NOT NULL,
  `task_param` TEXT NOT NULL,
  `insert_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP   ON  UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=INNODB CHARSET=utf8;


-- CREATE step_interval_percent
-- 步长占比表

CREATE TABLE `t_step_interval` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `task_id` int(11) NOT NULL COMMENT '关联的任务ID',
  `step_1_3` double NOT NULL,
  `step_4_6` double NOT NULL,
  `step_7_9` double NOT NULL,
  `step_10_30` double NOT NULL,
  `step_30_60` double NOT NULL,
  `step_60` double NOT NULL,
  `insert_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- CREATE step_interval_percent
-- 访问时长占比表
CREATE TABLE `t_cost_interval` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `task_id` INT(11) NOT NULL ,
  `cost_1_3` DOUBLE NOT NULL,
  `cost_4_6` DOUBLE NOT NULL,
  `cost_7_9` DOUBLE NOT NULL,
  `cost_10_30` DOUBLE NOT NULL,
  `cost_30_60` DOUBLE NOT NULL,
  `cost_1m_3m` DOUBLE NOT NULL,
  `cost_3m_10m` DOUBLE NOT NULL,
  `cost_10m_30m` DOUBLE NOT NULL,
  `insert_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;


-- 随机抽取session表

CREATE TABLE `t_session_extract` (
  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_id` INT(11) NOT NULL,
  `session_id` VARCHAR(255) NOT NULL,
  `start_time` VARCHAR(50) NOT NULL,
  `search_keywords` VARCHAR(255) DEFAULT NULL,
  `click_category_ids` VARCHAR(255) DEFAULT NULL,
  `insert_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP   ON  UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;


-- 商品品类TOP10

CREATE TABLE `t_top10_cate` (
  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_id` INT(11) NOT NULL,
  `cate_id` INT(11) NOT NULL,
  `click_cnt` BIGINT(20) NOT NULL,
  `pay_cnt` BIGINT(20) NOT NULL,
  `order_cnt` BIGINT(20) NOT NULL,
   `insert_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP   ON  UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;


-- 商品品类top10 session表
CREATE TABLE `t_top10_cate_session` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `task_id` int(11) NOT NULL,
  `category_id` int(11) NOT NULL,
  `session_id` varchar(255) NOT NULL,
  `click_cnt` bigint(20) NOT NULL,
  `insert_time` datetime DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
