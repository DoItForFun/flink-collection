package com.flyai.recommend.entity;

import java.io.Serializable;

/**
 * @author lizhe
 */
public class RedisActionEntity implements Serializable {
    public Integer getAction() {
        return action;
    }

    public void setAction(Integer action) {
        this.action = action;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public RedisActionEntity(Integer action, String key, String field, String value) {
        this.action = action;
        this.key = key;
        this.field = field;
        this.value = value;
    }

    private Integer action;
    private String key;
    private String field;
    private String value;

}
