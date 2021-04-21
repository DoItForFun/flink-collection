package com.flyai.recommend.entity;


import lombok.Data;

import java.util.List;

/**
 * @author lizhe
 */
@Data
public class ThreadEventEntity {
    private String id;
    private String thread_id;
    private String user_id;
    private int type;
    private int browse;
    private int detail;
    private int like;
    private int comment;
    private int share;
    private int collect;
    private Long timestamp;

    private String content_vector;
    private String create_time_vector;
    private String image_score_vector;
    private String is_selfie_vector;
    private String like_vector;
    private String word_count_vector;
    private String comment_vector;

    private String action_time_vector;
    private String carrier_vector;
    private String city_vector;
    private String manufacturer_vector;
    private String net_type_vector;
    private String os_vector;
    private String screen_height_vector;
    private String screen_width_vector;
    private String staff_vector;
    private String birthday_vector;
    private String is_vip_vector;
    private String register_time_vector;
    private String sex_vector;
    private String sun_vector;
    private String pay_vector;
}
