CREATE TABLE IF NOT EXISTS `magpie_leader_lock`
(
    `id`           INT         NOT NULL DEFAULT 1,
    `instance_id`  VARCHAR(32) NOT NULL COMMENT '持有锁的实例 UUID',
    `acquired_at`  DATETIME(3) NOT NULL COMMENT '获取锁时间',
    `heartbeat_at` DATETIME(3) NOT NULL COMMENT '最后心跳时间',

    PRIMARY KEY (`id`),
    CONSTRAINT `chk_single_row` CHECK (`id` = 1)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT 'Leader 锁';


CREATE TABLE IF NOT EXISTS `magpie_topic`
(
    `id`         CHAR(32)     NOT NULL COMMENT 'ID',
    `name`       VARCHAR(128) NOT NULL DEFAULT '' COMMENT '名称',
    `title`      VARCHAR(128) NOT NULL DEFAULT '' COMMENT '展示名称',
    `partitions` INT          NOT NULL DEFAULT 0 COMMENT '分区数',
    `properties` JSON         NOT NULL COMMENT '属性',
    `version`    INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name` (`name`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '消息主题';


CREATE TABLE IF NOT EXISTS `magpie_source`
(
    `id`         CHAR(32)     NOT NULL COMMENT 'ID',
    `type`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '类型',
    `name`       VARCHAR(128) NOT NULL DEFAULT '' COMMENT '名称',
    `title`      VARCHAR(128) NOT NULL DEFAULT '' COMMENT '展示名称',
    `is_enabled` BOOL         NOT NULL DEFAULT 0 COMMENT '是否启用',
    `properties` JSON         NOT NULL COMMENT '属性',
    `version`    INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name` (`name`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '消息来源';


CREATE TABLE IF NOT EXISTS `magpie_target`
(
    `id`         CHAR(32)     NOT NULL COMMENT 'ID',
    `type`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '类型',
    `name`       VARCHAR(128) NOT NULL DEFAULT '' COMMENT '名称',
    `title`      VARCHAR(128) NOT NULL DEFAULT '' COMMENT '展示名称',
    `topic`      VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息主题',
    `is_enabled` BOOL         NOT NULL DEFAULT 0 COMMENT '是否启用',
    `properties` JSON         NOT NULL COMMENT '属性',
    `version`    INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name` (`name`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '投递目标';


CREATE TABLE IF NOT EXISTS `magpie_consumer_offset`
(
    `id`         VARCHAR(128) NOT NULL COMMENT 'ID',
    `name`       VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消费者名称',
    `partition`  INT          NOT NULL DEFAULT 0 COMMENT '分区',
    `offset`     BIGINT       NOT NULL DEFAULT 0 COMMENT '消息偏移量',
    `version`    INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name_partition` (`name`, `partition`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '消息偏移量';


CREATE TABLE IF NOT EXISTS `magpie_message_log`
(
    `id`           CHAR(32)     NOT NULL COMMENT 'ID',
    `message_id`   CHAR(32)     NOT NULL COMMENT '消息 ID',
    `type`         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息类型',
    `event_time`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发生时间',
    `topic`        VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息主题',
    `tenant_id`    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '租户 ID',
    `business_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '业务键',
    `headers`      JSON         NOT NULL COMMENT '消息头',
    `payload`      MEDIUMTEXT   NOT NULL COMMENT '消息体',
    `created_at`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    INDEX `idx_message_id` (`message_id`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '消息记录';


CREATE TABLE IF NOT EXISTS `magpie_retry_message`
(
    `id`           CHAR(32)     NOT NULL COMMENT 'ID',
    `consumer`     VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消费者名称',
    `log_id`       CHAR(32)     NOT NULL DEFAULT '' COMMENT '消息记录 ID [FK|magpie_message_log.id]',
    `offset`       BIGINT       NOT NULL DEFAULT -1 COMMENT '消息偏移量',
    `attempts`     INT          NOT NULL DEFAULT 0 COMMENT '尝试次数',
    `retry_at`     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '下次可重试时间',
    `business_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '业务键',
    `version`      INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `created_at`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    INDEX `idx_consumer_retry_at` (`consumer`, `retry_at`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '待重试消息';
