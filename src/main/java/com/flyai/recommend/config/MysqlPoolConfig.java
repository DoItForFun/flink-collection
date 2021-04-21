package com.flyai.recommend.config;

import com.alibaba.druid.pool.DruidDataSource;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lizhe
 */
public class MysqlPoolConfig {
    private static DruidDataSource dataSource;
    private static ReentrantLock lockPool = new ReentrantLock();
    public static void initialPool() throws IOException {
        dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://172.21.16.39:3306/ai_data");
        dataSource.setUsername("cece");
        dataSource.setPassword("e3aXN4my377M8xU?");
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(20);
        dataSource.setMaxActive(500);
        dataSource.setMaxWait(20000);
        dataSource.setTimeBetweenEvictionRunsMillis(20000);
    }

    public static DruidDataSource getDataSource(){
        lockPool.lock();
        try {
            if (dataSource == null) {
                initialPool();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockPool.unlock();
        }
        return dataSource;
    }
}
