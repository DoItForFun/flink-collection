CREATE TABLE `cece_thread_vector`
(
    `id`                 char(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
    `content_vector`     text CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '帖子内容向量',
    `like_vector`        varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '点赞向量',
    `comment_vector`     varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '回复向量',
    `create_time_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '创建时间向量',
    `word_count_vector`  varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '字数向量',
    `image_score_vector` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '图片得分向量',
    `is_selfie_vector`    varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '是否为自拍向量',
    `topic_vector`       varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci            DEFAULT NULL COMMENT '话题向量',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;