package com.flyai.recommend.mapper;
import com.flyai.recommend.entity.RedisActionEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * @author lizhe
 */
public class MyRedisSink<IN> extends RichSinkFunction <IN> {

    private static final long serialVersionUID = 1L;
    private MyRedisMapper <IN> redisSinkMapper;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    public MyRedisSink(FlinkJedisConfigBase flinkJedisConfigBase, MyRedisMapper<IN> redisSinkMapper) {
        Preconditions.checkNotNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Preconditions.checkNotNull(redisSinkMapper, "Redis Mapper can not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisSinkMapper = redisSinkMapper;
    }

    public void invoke(IN input) throws Exception {
        String key = this.redisSinkMapper.getKeyFromData((RedisActionEntity) input);
        String value = this.redisSinkMapper.getValueFromData((RedisActionEntity) input);
        String additionalKey = this.redisSinkMapper.getKey((RedisActionEntity) input);
        RedisCommand command = this.redisSinkMapper.getCommand((RedisActionEntity) input);
        switch(command) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(additionalKey, value, key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(additionalKey, key, value);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type: " + command);
        }

    }

    public void open(Configuration parameters) throws Exception {
        this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
    }

    public void close() throws IOException {
        if (this.redisCommandsContainer != null) {
            this.redisCommandsContainer.close();
        }

    }
}
