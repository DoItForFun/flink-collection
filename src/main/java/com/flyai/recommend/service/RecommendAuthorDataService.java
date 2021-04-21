package com.flyai.recommend.service;

import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.utils.RedisUtils;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author lizhe
 */
@Slf4j
public class RecommendAuthorDataService {
    public static void incrementAuthor(String authorId , String field){
        try{
            Map<String, String> updateAuthorInfo = new HashMap<>();
            updateAuthorInfo.put("action" , RedisActionEnums.HIncrement.value().toString());
            updateAuthorInfo.put("key" , "recommendData:authorId:" + authorId);
            updateAuthorInfo.put("field" , field);
            RedisUtils.inputProcess(updateAuthorInfo);
        }catch (Exception e){
            log.error("author data {} data increment failed:{}" , field , e.getMessage());
        }
    }


    public static void savePublish(String authorId , String threadId){
        if(authorId == null || threadId == null){
            return;
        }
        try{
            Map <String, String> publishThreadSet = new HashMap <>();
            publishThreadSet.put("action" , RedisActionEnums.SAdd.value().toString());
            publishThreadSet.put("key" , "recommendData:authorThread:" + authorId);
            publishThreadSet.put("value" , threadId);
            RedisUtils.inputProcess(publishThreadSet);
        }catch (Exception e){
            log.error("author publish {}  save failed:{}" , threadId , e.getMessage());
        }
    }

    public static Set<String> getPublish(String authorId){
        try{
            Map <String, String> getPublishThreadSet = new HashMap <>();
            getPublishThreadSet.put("action" , RedisActionEnums.SGetAll.value().toString());
            getPublishThreadSet.put("key" , "recommendData:authorThread:" + authorId);
            return (Set <String>) RedisUtils.outputProcess(getPublishThreadSet);
        }catch (Exception e){
            log.error("author publish {}  save failed:{}" , authorId , e.getMessage());
            return null;
        }
    }

    public static Map<String, String> getAuthorData(String authorId){
        try{
            Map <String, String> selectAuthorData = new HashMap <>();
            selectAuthorData.put("action" , RedisActionEnums.HGetAll.value().toString());
            selectAuthorData.put("key" , "recommendData:authorId:" + authorId);
            return (Map <String, String>) RedisUtils.outputProcess(selectAuthorData);
        }catch (Exception e){
            log.error("author data {}  get failed:{}" , authorId , e.getMessage());
            return null;
        }
    }

    public static void setAuthData(String authorId , String field , String value){
        try{
            Map <String, String> setThreadField = new HashMap <>();
            setThreadField.put("action" , RedisActionEnums.HSet.value().toString());
            setThreadField.put("key" , "recommendData:authorId:" + authorId);
            setThreadField.put("field" , field);
            setThreadField.put("value" , value);
            RedisUtils.inputProcess(setThreadField);
        }catch (Exception e){
            log.error("author publish {}  save failed:{}" , authorId , e.getMessage());
        }
    }

}
