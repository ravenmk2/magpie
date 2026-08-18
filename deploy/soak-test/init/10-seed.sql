-- soak 环境连接器注册：topic / http source / mysql-poll source / http target。
-- 引擎 Coordinator 从 DB 读取期望状态并收敛，插入即注册。
-- 注意：mysql-poll source 的 password 需与 .env 的 MYSQL_ROOT_PASSWORD 保持一致（默认 magpie-soak）。

-- Topic：3 分区；stream 参数经 properties 透传——3 副本（配合 3 节点集群容忍单节点故障）、
-- 3 天保留期（防长跑写满磁盘）
INSERT INTO `magpie_topic` (`id`, `name`, `title`, `partitions`, `properties`)
VALUES (REPLACE(UUID(), '-', ''), 'soak-ordered', 'soak 有序', 3,
        JSON_OBJECT('x-initial-cluster-size', '3', 'x-max-age', '3D')),
       (REPLACE(UUID(), '-', ''), 'soak-key-ordered', 'soak 键有序', 3,
        JSON_OBJECT('x-initial-cluster-size', '3', 'x-max-age', '3D')),
       (REPLACE(UUID(), '-', ''), 'soak-best-effort', 'soak 尽力', 3,
        JSON_OBJECT('x-initial-cluster-size', '3', 'x-max-age', '3D')),
       (REPLACE(UUID(), '-', ''), 'soak-outbox', 'soak outbox', 3,
        JSON_OBJECT('x-initial-cluster-size', '3', 'x-max-age', '3D'));

-- HTTP source：loadgen 发布入口
INSERT INTO `magpie_source` (`id`, `type`, `name`, `title`, `is_enabled`, `properties`)
VALUES (REPLACE(UUID(), '-', ''), 'http', 'soak-http', 'soak 发布入口', 1,
        JSON_OBJECT('allowedTopics',
                    JSON_ARRAY('soak-ordered', 'soak-key-ordered', 'soak-best-effort')));

-- MySQL outbox source：覆盖 mysql-poll 链路（password 与 MYSQL_ROOT_PASSWORD 一致）
INSERT INTO `magpie_source` (`id`, `type`, `name`, `title`, `is_enabled`, `properties`)
VALUES (REPLACE(UUID(), '-', ''), 'mysql-poll', 'soak-outbox', 'soak outbox 轮询', 1,
        JSON_OBJECT('url', 'jdbc:mysql://mysql:3306/magpie?useSSL=false&characterEncoding=UTF-8',
                    'username', 'root',
                    'password', 'magpie-soak',
                    'pollInterval', 1000));

-- Target：三种投递模式 + outbox 链路，全部指向 verifier 的对应通道
INSERT INTO `magpie_target` (`id`, `type`, `name`, `title`, `topic`, `is_enabled`, `properties`)
VALUES (REPLACE(UUID(), '-', ''), 'http', 'soak-verifier-ordered', 'soak 校验 ORDERED', 'soak-ordered', 1,
        JSON_OBJECT('url', 'http://verifier:8080/events/ordered', 'deliveryMode', 'ORDERED')),
       (REPLACE(UUID(), '-', ''), 'http', 'soak-verifier-key-ordered', 'soak 校验 KEY_ORDERED', 'soak-key-ordered', 1,
        JSON_OBJECT('url', 'http://verifier:8080/events/key-ordered', 'deliveryMode', 'KEY_ORDERED')),
       (REPLACE(UUID(), '-', ''), 'http', 'soak-verifier-best-effort', 'soak 校验 BEST_EFFORT', 'soak-best-effort', 1,
        JSON_OBJECT('url', 'http://verifier:8080/events/best-effort', 'deliveryMode', 'BEST_EFFORT')),
       (REPLACE(UUID(), '-', ''), 'http', 'soak-verifier-outbox', 'soak 校验 outbox', 'soak-outbox', 1,
        JSON_OBJECT('url', 'http://verifier:8080/events/outbox', 'deliveryMode', 'BEST_EFFORT'));
