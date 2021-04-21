package com.flyai.recommend.asyn;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.classification.DataClassification;
import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.enums.AuthorEnums;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.handler.GetThreadHandler;
import com.flyai.recommend.utils.RedisUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;

/**
 * @author lizhe
 */
public class MyAsync2Function {

    public static void Browse(ThreadActionEntity threadActionEntity){
        RedisActionEntity redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadActionEntity.getThreadId(), EventEnums.Browse.desc(), threadActionEntity.getCount().toString());

        RedisUtils.inputProcess(redisActionEntity);
    }


    public void threadUpdate(ThreadActionEntity threadActionEntity) throws InterruptedException {
        RedisActionEntity redisActionEntity = null;
        String threadPrefix = "recommendData:threadId:";
        String threadId = threadActionEntity.getThreadId();
        String authorId = null;
        switch (threadActionEntity.getAction()) {
            case "Browse":
                redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadId, EventEnums.Browse.desc(), threadActionEntity.getCount().toString());
                break;
            case "Browsed":
                authorId = getAuthorId(threadId);
                if (authorId != null) {
                    redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:authorId:" + authorId, AuthorEnums.Browse.desc(), threadActionEntity.getCount().toString());
                }
                break;
            case "Detail":
                redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadId, EventEnums.Detail.desc(), threadActionEntity.getCount().toString());
                break;
            case "PublishSave":
                authorId = getAuthorId(threadId);
                if (authorId != null) {
                    redisActionEntity = new RedisActionEntity(RedisActionEnums.SAdd.value(), "recommendData:authorThread:" + authorId, null, threadId);
                }
                break;
            case "Publish":
                authorId = getAuthorId(threadId);
                if (authorId != null) {
                    redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:authorId:" + authorId, AuthorEnums.Publish.desc(), threadActionEntity.getCount().toString());
                }
                break;
            case "Comment":
                redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadId, EventEnums.Comment.desc(), threadActionEntity.getCount().toString());
                break;
            case "Share":
                redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadId, EventEnums.Share.desc(), threadActionEntity.getCount().toString());
                break;
            case "Like":
                redisActionEntity = new RedisActionEntity(RedisActionEnums.HIncrement.value(), "recommendData:threadId:" + threadId, EventEnums.Like.desc(), threadActionEntity.getCount().toString());
                break;
            default:
                break;
        }

        if(redisActionEntity != null && redisActionEntity.getField().equals("Like")){
            System.err.println(redisActionEntity.getField());
        }
//        if (redisActionEntity != null) {
//            Thread.sleep(5000);
            RedisUtils.inputProcess(redisActionEntity);
//        }
    }


    public String getAuthorId(String threadId) {
        ThreadEntity threadEntity = GetThreadHandler.process(threadId);
        if (threadEntity == null || threadEntity.getAuthorId() == null) {
            return null;
        }
        return threadEntity.getAuthorId();
    }
}
