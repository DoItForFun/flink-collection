package com.flyai.recommend.config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lizhe
 */
public class RedisClient {
    private static ReentrantLock lockJedis = new ReentrantLock();
    private static JedisPool redisPool = null;

    /**
     * 获取redis实例
     */
    public static Jedis getJedis() {
        // 尝试获取redis实例锁，适用于高并发多线程场景
        lockJedis.lock();
        Jedis jedis = null;
        try {
            if (redisPool == null) {
                redisPool = RedisPoolConfig.getRedisPool();
            }
            jedis = redisPool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockJedis.unlock();
        }
        return jedis;
    }
    /**
     * 释放连接
     */
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
}
