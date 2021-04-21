package com.flyai.recommend.config;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lizhe
 */
@Slf4j
public class MysqlClient {
    private static ReentrantLock lockMysql = new ReentrantLock();
    private static DataSource dataSource;
    public static Connection getConnect(){
        lockMysql.lock();
        Connection connection = null;
        try{
            if(dataSource == null){
               dataSource = MysqlPoolConfig.getDataSource();
            }
            connection = dataSource.getConnection();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            lockMysql.unlock();
        }
        return connection;
    }
    public static void close(Connection connection){
        if(connection != null){
            try{
                if(connection != null){
                    connection.close();
                }
            } catch (SQLException throwables) {
                log.error("close connect failed:{}" , throwables.getMessage());
            }
        }
    }
}
