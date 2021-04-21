package com.flyai.recommend.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class BaseInfoEntity {
    @JSONField(name = "action_id")
    private String actionId;
    @JSONField(name = "action_time")
    private Long actionTime;
    @JSONField(name = "item_id")
    private Object itemId;
    @JSONField(name = "user_id")
    private String userId;
}
