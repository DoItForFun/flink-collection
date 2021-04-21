package com.flyai.recommend.entity;

import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class UserVectorEntity {
    private String id;
    private String birthday_vector;
    private String sex_vector;
    private String sun_vector;
    private String is_vip_vector;
    private String register_time_vector;
    private String action_time_vector;
    private String city_vector;
    private String manufacturer_vector;
    private String os_vector;
    private String screen_height_vector;
    private String screen_width_vector;
    private String network_type_vector;
    private String carrier_vector;
    private String staff_vector;
    private String pay_vector;
}
