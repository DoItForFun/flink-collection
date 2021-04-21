package com.flyai.recommend.utils;


import com.flyai.recommend.config.RedisClient;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.enums.RedisActionEnums;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.Set;

@Slf4j
public class RedisUtils {
    public static void inputProcess(Pipeline pipeline , Map <String, String> params) {
        Jedis resource = null;
        try {
            resource = RedisClient.getJedis();
            RedisActionEnums redisActionEnums = RedisActionEnums.from(Integer.valueOf(params.get("action")));
            switch (redisActionEnums) {
                case Set:
                    set(pipeline, params.get("key"), params.get("value"));
                    break;
                case HSet:
                    hSet(pipeline, params.get("key"), params.get("field"), params.get("value"));
                    break;
                case HIncrement:
                    hIncrement(pipeline, params.get("key"), params.get("field"), "1");
                    break;
                case LPush:
                    LPush(pipeline, params.get("key"), params.get("value"));
                    break;
                case SAdd:
                    sAdd(pipeline, params.get("key"), params.get("value"));
                    break;
            }
        } catch (Exception e) {
            log.error("redis input {} error:{}", params, e.getMessage());
        }finally {

            RedisClient.closeJedis(resource);
        }
    }

    public static void inputProcess(Map <String, String> params) {
        Jedis resource = null;

        try {
            resource = RedisClient.getJedis();
            RedisActionEnums redisActionEnums = RedisActionEnums.from(Integer.valueOf(params.get("action")));
            switch (redisActionEnums) {
                case Set:
                    set(resource, params.get("key"), params.get("value"));
                    break;
                case HSet:
                    hSet(resource, params.get("key"), params.get("field"), params.get("value"));
                    break;
                case HIncrement:
                    hIncrement(resource, params.get("key"), params.get("field"), "1");
                    break;
                case LPush:
                    LPush(resource, params.get("key"), params.get("value"));
                    break;
                case SAdd:
                    sAdd(resource, params.get("key"), params.get("value"));
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            RedisClient.closeJedis(resource);
        }
    }

    public static void inputProcess(RedisActionEntity redisActionEntity) {
        Jedis resource = null;
        try {
            resource = RedisClient.getJedis();
            resource.select(3);
            RedisActionEnums redisActionEnums = RedisActionEnums.from(redisActionEntity.getAction());
            switch (redisActionEnums) {
                case Set:
                    set(resource, redisActionEntity.getKey(), redisActionEntity.getValue());
                    break;
                case HSet:
                    hSet(resource, redisActionEntity.getKey(), redisActionEntity.getField(), redisActionEntity.getValue());
                    break;
                case HIncrement:
                    hIncrement(resource, redisActionEntity.getKey(), redisActionEntity.getField(), redisActionEntity.getValue());
                    break;
                case LPush:
                    LPush(resource, redisActionEntity.getKey(), redisActionEntity.getValue());
                    break;
                case SAdd:
                    sAdd(resource, redisActionEntity.getKey(), redisActionEntity.getValue());
                    break;
            }
        } catch (Exception e) {
            log.error("redis input {} error:{}", redisActionEntity, e.getMessage());
        } finally {
            RedisClient.closeJedis(resource);
        }
    }

    public static Object outputProcess(Map <String, String> params) {
        Jedis resource = null;
        try {
            resource = RedisClient.getJedis();
            RedisActionEnums redisActionEnums = RedisActionEnums.from(Integer.valueOf(params.get("action")));
            switch (redisActionEnums) {
                case Get:
                    return get(resource, params.get("key"));
                case HGet:
                    return hGet(resource, params.get("key"), params.get("field"));
                case HGetAll:
                    Map <String, String> map = hGetAll(resource, params.get("key"));
                    if(!map.isEmpty()){
                        System.err.println("map:" + map);
                        return map;
                    }
                    break;
                case SGetAll:
                    return SGetAll(resource, params.get("key"));
                default:
                    throw new IllegalArgumentException("illegal action value");
            }
        } catch (Exception e) {
            log.error("redis output error:{}", e.getMessage());
        } finally {
            RedisClient.closeJedis(resource);
        }
        return null;
    }

    private static void set(Jedis resource, String key, String value) {
        resource.set(key, value);
    }

    private static void set(Pipeline pipeline, String key, String value) {
        pipeline.set(key, value);
    }

    private static void hSet(Jedis resource, String key, String field, String value) {
        resource.hset(key, field, value);
    }

    private static void hSet(Pipeline pipeline, String key, String field, String value) {
        pipeline.hset(key, field, value);
    }

    private static void sAdd(Jedis resource, String key, String value) {
        resource.sadd(key, value);
    }

    private static void sAdd(Pipeline pipeline, String key, String value) {
        pipeline.sadd(key, value);
    }

    private static String get(Jedis resource, String key) {
        return resource.get(key);
    }

    private static String hGet(Jedis resource, String key, String field) {
        return resource.hget(key, field);
    }

    private static String hMSet(Jedis resource, String key, Map<String, String> field){
        return resource.hmset(key , field);
    }

    private static Map <String, String> hGetAll(Jedis resource, String key) {
        return resource.hgetAll(key);
    }

    private static void hIncrement(Jedis resource, String key, String field, String value) {
        resource.hincrBy(key, field, Long.parseLong(value));
    }

    private static void hIncrement(Pipeline pipeline, String key, String field, String value) {
        pipeline.hincrBy(key, field, Long.parseLong(value));
    }

    private static void LPush(Jedis resource, String key, String value) {
        resource.lpush(key, value);
    }

    private static void LPush(Pipeline pipeline, String key, String value) {
        pipeline.lpush(key, value);
    }

    private static Set <String> SGetAll(Jedis resource, String key) {
        return resource.smembers(key);
    }
}
