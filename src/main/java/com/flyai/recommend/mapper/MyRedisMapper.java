package com.flyai.recommend.mapper;


import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author lizhe
 */
public class MyRedisMapper<T> implements RedisMapper<com.flyai.recommend.entity.RedisActionEntity> {


    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET , "null");
    }

    @Override
    public String getKeyFromData(com.flyai.recommend.entity.RedisActionEntity o) {
        return o.getField();
    }

    @Override
    public String getValueFromData(com.flyai.recommend.entity.RedisActionEntity o) {
        return o.getValue();
    }

    public String getKey(com.flyai.recommend.entity.RedisActionEntity o){
        return o.getKey();
    }

    public RedisCommand getCommand(com.flyai.recommend.entity.RedisActionEntity o){
        switch (o.getAction()){
            case 20:
                return RedisCommand.HSET;
            case 40:
                return RedisCommand.LPUSH;
        }
        return RedisCommand.HSET;
    }

}
