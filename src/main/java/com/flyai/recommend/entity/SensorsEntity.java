package com.flyai.recommend.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class SensorsEntity {
    @JSONField(name = "distinct_id")
    private String distinctId;
    private String event;
    @JSONField(name = "properties")
    private String properties;
    private Long time;
    private String type;
}
