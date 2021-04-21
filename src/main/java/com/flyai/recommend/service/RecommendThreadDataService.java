package com.flyai.recommend.service;

import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lizhe
 */
@Slf4j
public class RecommendThreadDataService {
    public static void incrementThread(String threadId , String field){
        try{
            Map <String , String> params = new HashMap <>();
            params.put("action" , RedisActionEnums.HIncrement.value().toString());
            params.put("key" , "recommendData:threadId:" + threadId);
            params.put("field" , field);
            RedisUtils.inputProcess(params);
        }catch (Exception e){
            log.error("{} data increment failed:{}" , field , e.getMessage());
        }
    }

    public static void threadFieldSet(String threadId , String field , String value){
        try{
            Map <String , String> params = new HashMap <>();
            params.put("action" , RedisActionEnums.HSet.value().toString());
            params.put("key" , "recommendData:threadId:" + threadId);
            params.put("field" , field);
            params.put("value" , value);
            RedisUtils.inputProcess(params);
        }catch (Exception e){
            log.error("{} data set failed:{}" , field , e.getMessage());
        }
    }

    public static void threadFieldSet(Pipeline pipeline , String threadId , String field , String value){
        try{
            Map <String , String> params = new HashMap <>();
            params.put("action" , RedisActionEnums.HSet.value().toString());
            params.put("key" , "recommendData:threadId:" + threadId);
            params.put("field" , field);
            params.put("value" , value);
            RedisUtils.inputProcess(pipeline , params);
        }catch (Exception e){
            log.error("{} data set failed:{}" , field , e.getMessage());
        }
    }

    public static Map <String, String> threadHGetAll(String threadId){
        try{
            Map <String , String> params = new HashMap <>();
            params.put("action" , RedisActionEnums.HGetAll.value().toString());
            params.put("key" , "recommendData:threadId:" + threadId);
            return (Map <String, String>) RedisUtils.outputProcess(params);
        }catch (Exception e){
            log.error("{} data set failed:{}" , threadId , e.getMessage());
            return null;
        }
    }

    public static void threadLPush(String threadId){
        try{
            Map <String , String> params = new HashMap <>();
            params.put("action" , RedisActionEnums.LPush.value().toString());
            params.put("key" , "recommendData:updateThreadList");
            params.put("value" , threadId);
            RedisUtils.inputProcess(params);
        }catch (Exception e){
            log.error("{} save processed data failed:{}" , threadId , e.getMessage());
        }
    }
}
