package com.flyai.recommend.mapper;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisClusterContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisContainer;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * @author lizhe
 */
public class MyRedisCluster implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisContainer.class);

    private final JedisPool jedisPool;
    private final JedisSentinelPool jedisSentinelPool;


    public MyRedisCluster(JedisPool jedisPool) {
        Preconditions.checkNotNull(jedisPool, "Jedis Pool can not be null");
        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }
    public synchronized void hIncrement(final String key, String hashField, final String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.hincrBy(key, hashField, Long.parseLong(value));
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HIncrement to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public Map <String, String> hGetAll(final String key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGetAll to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    public void hMSet(final  String key , final Map<String, String> hash){
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.hmset(key , hash);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGetAll to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }
    private Jedis getInstance() {
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            LOG.error("Failed to close (return) instance to pool", e);
        }
    }

    @Override
    public void hset(String key, String hashField, String value) {

    }

    @Override
    public void rpush(String listName, String value) {

    }

    @Override
    public void lpush(String listName, String value) {

    }

    @Override
    public synchronized void sadd(String setName, String value) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            jedis.sadd(setName , value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HIncrement to set {} error message {}",
                        setName, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void publish(String channelName, String message) {

    }

    @Override
    public void set(String key, String value) {

    }

    @Override
    public void pfadd(String key, String element) {

    }

    @Override
    public void zadd(String key, String score, String element) {

    }

    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }
    }

}
