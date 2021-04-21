package com.flyai.recommend.mapper;

import com.alibaba.fastjson.JSONObject;
import com.flyai.recommend.config.RedisPoolConfig;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.enums.AuthorEnums;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.handler.GetThreadHandler;
import com.flyai.recommend.utils.RedisUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author lizhe
 */
public class MyRichAsyFunction extends RichAsyncFunction<ThreadActionEntity, RedisActionEntity> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyRichAsyFunction.class);

    @Override
    public void asyncInvoke(ThreadActionEntity threadActionEntity, ResultFuture<RedisActionEntity> resultFuture) throws Exception {

        CompletableFuture.supplyAsync(new Supplier<RedisActionEntity>() {
            @Override
            public RedisActionEntity get() {
                RedisActionEntity redisActionEntity = null;
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
                return redisActionEntity;
            }
        }).thenAccept((RedisActionEntity result) -> {
            if(result != null){
                System.err.println(result.getField() + ":" + result.getKey());
//                RedisUtils.inputProcess(result);
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ThreadActionEntity input, ResultFuture resultFuture) throws Exception {
        System.err.println(input);
        LOGGER.error("timeout !");
    }

    public static String getAuthorId(String threadId) {
        ThreadEntity threadEntity = GetThreadHandler.process(threadId);
        if (threadEntity == null || threadEntity.getAuthorId() == null) {
            return null;
        }
        return threadEntity.getAuthorId();
    }
}
