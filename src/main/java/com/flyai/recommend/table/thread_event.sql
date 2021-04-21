CREATE TABLE `thread_event`
(
    `id`             int(255) unsigned                                       NOT NULL AUTO_INCREMENT,
    `user_id`        varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '用户ID',
    `thread_id`      varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '帖子ID',
    `type`           int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否正样本',
    `browse`         int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否曝光',
    `detail`         int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否浏览',
    `like`           int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否点赞',
    `comment`        int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否回复',
    `share`          int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否分享',
    `collect`        int(11)                                                 NOT NULL DEFAULT 0 COMMENT '是否收藏',
    `timestamp`      int(255)                                                NOT NULL COMMENT '更新时间戳',
    `content_vector` text CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子内容向量',
    `create_time_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子创建时间向量',
    `image_score_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子图片分向量',
    `is_selfie_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子图片是否为自拍向量',
    `like_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子点赞数向量',
    `word_count_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子字数向量',
    `comment_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '帖子回复向量',
    `action_time_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户活跃星期向量',
    `carrier_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户运营商向量',
    `city_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户所在城市等级向量',
    `manufacturer_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户手机品牌向量',
    `net_type_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户网络类型向量',
    `os_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户手机系统向量',
    `screen_height_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户手机屏高向量',
    `screen_width_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户手机屏款向量',
    `staff_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户是否为达人向量',
    `birthday_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户出生日星期向量',
    `is_vip_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户是否为vip向量',
    `register_time_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户注册时间向量',
    `sex_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户性别向量',
    `sun_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户太阳星座向量',
    `pay_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '用户支付金额向量',
    PRIMARY KEY (`id`),
    KEY `search` (`user_id`, `thread_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;