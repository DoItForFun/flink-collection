package com.flyai.recommend.mapper;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.enums.RedisActionEnums;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.Map;

/**
 * @author lizhe
 */
@Slf4j
public class MyRedisSink3<IN> extends RichSinkFunction <IN> {
    private static final long serialVersionUID = 1L;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private MyRedisCluster redisCommandsContainer;

    public MyRedisSink3(FlinkJedisConfigBase flinkJedisConfigBase) {
        Preconditions.checkNotNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
    }

    public void invoke(IN input) throws Exception {
        RedisActionEntity redisActionEntity = (RedisActionEntity)input;
        Integer actionId = redisActionEntity.getAction();
        RedisActionEnums redisActionEnums = RedisActionEnums.from(actionId);
        switch (redisActionEnums){
            case HMSet:
                Map<String , String>  map = JSON.parseObject(redisActionEntity.getValue() , Map.class);
                this.redisCommandsContainer.hMSet(redisActionEntity.getKey() , map);
                break;
            default:
                log.error("classification failed:{}" , redisActionEntity.toString());
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
}
