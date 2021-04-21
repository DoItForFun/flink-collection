CREATE TABLE `user`
(
    `id`            char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
    `nick_name`     varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '昵称',
    `birthday`      varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '出生日期',
    `sex`           varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '性别',
    `phone_number`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '手机号',
    `birthplace`    varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '出生地',
    `location`      varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '所在地',
    `sun`           varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '太阳星座',
    `is_vip`        int(11)                                                            DEFAULT NULL COMMENT '是否是vip',
    `register_time` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '注册时间',
    `country`       varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '国家',
    `province`      varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '省份',
    `city`          varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '城市',
    `manufacturer`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '手机品牌',
    `os`            varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '系统',
    `device_id`     varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '设备id',
    `screen_height` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '屏高',
    `screen_width`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '屏宽',
    `network_type`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '网络类型',
    `carrier`       varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '运营商',
    `pay`           varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '付费',
    `staff`         int(11)                                                            DEFAULT NULL COMMENT '是否为达人',
    `ip`            varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT 'ip',
    `action_time`   varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '最后活跃时间',

    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;