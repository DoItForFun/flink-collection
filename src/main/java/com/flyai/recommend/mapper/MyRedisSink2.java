package com.flyai.recommend.mapper;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.entity.ReduceActionEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.handler.GetThreadHandler;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.container.RedisContainer;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

/**
 * @author lizhe
 */
public class MyRedisSink2<IN> extends RichSinkFunction <IN> {

    private static final long serialVersionUID = 1L;
    private MyRedisMapper <IN> redisSinkMapper;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private MyRedisCluster redisCommandsContainer;

    public MyRedisSink2(FlinkJedisConfigBase flinkJedisConfigBase, MyRedisMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisSinkMapper = redisSinkMapper;
    }

    public void invoke(IN input) throws Exception {
        ReduceActionEntity reduceActionEntity =  (ReduceActionEntity) input;
        String threadId = reduceActionEntity.getThreadId();
        if(reduceActionEntity.getBrowsed() != null
                || reduceActionEntity.getPublish() != null
                || reduceActionEntity.getPublishSave() != null
        ){
            String authorId = getAuthorId(reduceActionEntity.getThreadId());
            if(reduceActionEntity.getBrowsed() != null){
                String keyBrowsed = "recommendData:authorId:" + authorId;
                this.redisCommandsContainer.hIncrement(keyBrowsed , "Browsed" , reduceActionEntity.getBrowsed().toString());
            }
            if(reduceActionEntity.getPublishSave() != null){
                String keyPublishSave = "recommendData:authorThread:" + authorId;
                this.redisCommandsContainer.sadd(keyPublishSave , threadId);
            }
            if(reduceActionEntity.getPublish() != null){
                String keyPublish = "recommendData:authorId:" + authorId;
                this.redisCommandsContainer.hIncrement(keyPublish , "Publish" , reduceActionEntity.getPublish().toString());
            }
        }
        if(reduceActionEntity.getBrowse() != null
                ||reduceActionEntity.getLike() != null
                || reduceActionEntity.getDetail() != null
                || reduceActionEntity.getShare() != null
                || reduceActionEntity.getComment() != null
        ){
            Map <String , String> updateMap = new HashMap<>();
            if(reduceActionEntity.getBrowse() != null){
                updateMap.put("Browse" , reduceActionEntity.getBrowse().toString());
            }
            if(reduceActionEntity.getComment() != null){
                updateMap.put("Comment" , reduceActionEntity.getComment().toString());
            }
            if(reduceActionEntity.getShare() != null){
                updateMap.put("Share" , reduceActionEntity.getShare().toString());
            }
            if(reduceActionEntity.getLike() != null){
                updateMap.put("Like" , reduceActionEntity.getLike().toString());
            }
            if(reduceActionEntity.getDetail() != null){
                updateMap.put("Detail" , reduceActionEntity.getDetail().toString());
            }
            String key = "recommendData:threadId:" + threadId;
            Map<String , String> currentMap = this.redisCommandsContainer.hGetAll(key);
            if(currentMap != null){
                for(Map.Entry<String, String> entry : currentMap.entrySet()){
                    if(updateMap.get(entry.getKey()) != null){
                        int increment = Integer.parseInt(updateMap.get(entry.getKey()));
                        int current = entry.getValue() == null ? 0 : Integer.parseInt(entry.getValue());
                        updateMap.put(entry.getKey() , String.valueOf(Math.addExact(increment , current)));
                    }else{
                        updateMap.put(entry.getKey() , entry.getValue());
                    }
                }
            }
            this.redisCommandsContainer.hMSet(key , updateMap);
        }
    }

    public void open(Configuration parameters) throws Exception {
        Preconditions.checkNotNull(parameters, "Redis pool config should not be Null");
        FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(flinkJedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(flinkJedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(flinkJedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, flinkJedisPoolConfig.getHost(),
                flinkJedisPoolConfig.getPort(), flinkJedisPoolConfig.getConnectionTimeout(), flinkJedisPoolConfig.getPassword(),
                flinkJedisPoolConfig.getDatabase());
        this.redisCommandsContainer = new MyRedisCluster(jedisPool);
    }

    public void close() throws IOException {
        if (this.redisCommandsContainer != null) {
            this.redisCommandsContainer.close();
        }

    }

    public String getAuthorId(String threadId) {
        ThreadEntity threadEntity = GetThreadHandler.process(threadId);
        if (threadEntity == null || threadEntity.getAuthorId() == null) {
            return null;
        }
        return threadEntity.getAuthorId();
    }
}
