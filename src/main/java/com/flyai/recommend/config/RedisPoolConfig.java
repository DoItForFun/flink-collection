package com.flyai.recommend.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lizhe
 */
public class RedisPoolConfig {
    private static ReentrantLock lockPool = new ReentrantLock();
    private static JedisPool jedisPool;


    /**
     * 初始化Jedis连接池
     * @throws IOException
     */
    public static void initialPool() throws IOException {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(5000);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(1);
        poolConfig.setTestOnCreate(false);
        jedisPool = new JedisPool(poolConfig, "10.19.52.120", 6379, 0, "cece_xxwolo");
    }
    /**
     * 初始化连接池
     * @return
     * @throws IOException
     */
    public static JedisPool getRedisPool() {
        lockPool.lock();
        try {
            if (jedisPool == null) {
                initialPool();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockPool.unlock();
        }
        return jedisPool;
    }

}
