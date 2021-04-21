package com.flyai.recommend.entity;

import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class UserEntity {
    private String id;
    private String nick_name;
    private Long birthday;
    private String sex;
    private String phone_number;
    private String birthplace;
    private String location;
    private String sun;
    private Integer is_vip;
    private Long register_time;
    private String country;
    private String province;
    private String city;
    private String manufacturer;
    private String os;
    private String device_id;
    private String screen_height;
    private String screen_width;
    private String network_type;
    private String carrier;
    private Integer staff;
    private String pay;
    private String ip;
    private Long action_time;
}
