package com.flyai.recommend.enums;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Objects;

/**
 * @author lizhe
 */
public enum RedisActionEnums {

    Set(10, "set"),
    Get(11, "get"),
    Increment(12 , "Increment"),

    HSet(20, "HSet"),
    HGet(21, "HGet"),
    HGetAll(22, "HGetAll"),
    HIncrement(23 , "HIncrement"),
    HMSet(24 , "HMSet"),


    LPush(40 , "LPush"),

    SAdd(50, "HSet"),
    SGetAll(51 , "SGetAll");

    private Integer value;
    private String desc;

    RedisActionEnums(Integer value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public static RedisActionEnums from(Integer value) {
        for (RedisActionEnums status : RedisActionEnums.values()) {
            if (Objects.equal(status.value(), value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("illegal state value");
    }

    public Integer value() {
        return this.value;
    }

    public String desc() {
        return this.desc;
    }

}
