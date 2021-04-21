package com.flyai.recommend.handler;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.utils.RedisUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizhe
 */
public class GetThreadHandler {
    public static ThreadEntity process(String threadId){
        Map <String, String> selectParams = new HashMap <>();
        selectParams.put("action" , RedisActionEnums.HGet.value().toString());
        selectParams.put("key" , "threadInfo::cece");
        selectParams.put("field" , threadId);
        String threadInfo = (String) RedisUtils.outputProcess(selectParams);
        try{
            return JSON.parseObject(threadInfo, ThreadEntity.class);
        }catch (Exception e){
            return null;
        }
    }
}
