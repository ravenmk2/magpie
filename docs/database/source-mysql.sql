-- MySQL Source 轮询 outbox 表。
-- 投递按 (created_at, id) 升序；轮询只处理 created_at 早于 now - readLag 的行，
-- 因此业务事务从 INSERT 到 COMMIT 的时长必须小于连接器的 readLag 配置（默认 500ms），
CREATE TABLE IF NOT EXISTS `magpie_outbox_message`
(
    `id`           CHAR(32)     NOT NULL COMMENT 'ID',
    `type`         VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息类型',
    `event_time`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发生时间',
    `topic`        VARCHAR(128) NOT NULL DEFAULT '' COMMENT '消息主题',
    `tenant_id`    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '租户 ID',
    `business_key` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '业务键',
    `headers`      JSON         NOT NULL COMMENT '消息头',
    `payload`      MEDIUMTEXT   NOT NULL COMMENT '消息体',
    `created_at`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '写入时间（DB 时钟，投递排序键，业务勿写）',

    PRIMARY KEY (`id`),
    INDEX `idx_created_at_id` (`created_at`, `id`)
)
    ENGINE = InnoDB
    CHARSET = utf8mb4
    COMMENT '待发布消息';
