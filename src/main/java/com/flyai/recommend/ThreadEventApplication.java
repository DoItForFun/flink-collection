package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flyai.recommend.config.MysqlClient;
import com.flyai.recommend.config.MysqlPoolConfig;
import com.flyai.recommend.entity.*;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.mapper.MyMongodbSink2;
import com.flyai.recommend.mapper.MyTiDbSink;
import com.flyai.recommend.utils.DruidUtils;
import com.flyai.recommend.utils.MongodbUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class ThreadEventApplication {
    private static StreamExecutionEnvironment env;
    private static DataStream <String> dataStream;

    public static void main(String[] args) {
        try {
            setEnv();
            StreamExecutionEnvironment streamExecutionEnvironment = setCheckPoint(env);
            setStreamDataSource(env);
            process();
            env.execute("save event job start");
        } catch (Exception e) {
            log.error("job run exception:{}", Throwables.getStackTraceAsString(e));
        }
    }

    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static StreamExecutionEnvironment setCheckPoint(StreamExecutionEnvironment env) throws IOException {
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    private static void setStreamDataSource(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.load(RecommendApplication.class.getResourceAsStream("/application.properties"));
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"));
        kafkaProperties.setProperty("group.id", properties.getProperty("kafka.event.consumer.thread.group"));
        kafkaProperties.setProperty("enable.auto.commit", "false");
        String topics = properties.getProperty("kafka.event.consumer.topic");
        FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <String>(topics, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();
        dataStream = env.addSource(consumer);
    }

    private static void process() {
        dataStream
                .filter(new FilterFunction <String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        if (s != null && !s.isEmpty()) {
                            return true;
                        }
                        return false;
                    }
                })
                .map((MapFunction <String, BaseInfoEntity>) s -> {
                    Map map = JSON.parseObject(s, Map.class);
                    if (!map.containsKey("properties")) {
                        return null;
                    }
                    Map data = JSON.parseObject(map.get("properties").toString(), Map.class);
                    if (!data.containsKey("properties")) {
                        return null;
                    }
                    return JSON.parseObject(data.get("properties").toString(), BaseInfoEntity.class);
                })
                .filter(new FilterFunction <BaseInfoEntity>() {
                    @Override
                    public boolean filter(BaseInfoEntity baseInfoEntity) throws Exception {
                        if (baseInfoEntity != null
                                && baseInfoEntity.getActionId() != null
                                && (Integer.parseInt(baseInfoEntity.getActionId()) == 0
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 3
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 4
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 5
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 6
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 11
                                || Integer.parseInt(baseInfoEntity.getActionId()) == 12
                        )
                        ) {
                            return true;
                        }
                        return false;
                    }
                })
                .flatMap(new FlatMapFunction <BaseInfoEntity, ThreadEventEntity>() {
                    @Override
                    public void flatMap(BaseInfoEntity baseInfoEntity, Collector <ThreadEventEntity> collector) throws Exception {
                        EventEnums eventEnums = EventEnums.from(Integer.parseInt(baseInfoEntity.getActionId()));
                        if (eventEnums != null) {
                            switch (eventEnums) {
                                case Browse:
                                    browse(baseInfoEntity, collector);
                                    break;
                                case Detail:
                                case Like:
                                case Share:
                                case Comment:
                                case Collect:
                                    common(baseInfoEntity, collector, eventEnums);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                })
                .keyBy("id")
                .timeWindow(Time.seconds(10))
                .reduce(new ReduceFunction <ThreadEventEntity>() {
                    @Override
                    public ThreadEventEntity reduce(ThreadEventEntity threadEventEntity, ThreadEventEntity t1) throws Exception {
                        if (threadEventEntity.getId().equals(t1.getId())) {
                            if (t1.getTimestamp() > threadEventEntity.getTimestamp()) {
                                threadEventEntity.setTimestamp(t1.getTimestamp());
                            }
                            if (threadEventEntity.getType() != t1.getType() && t1.getType() == 1) {
                                threadEventEntity.setType(1);
                            }
                            if (threadEventEntity.getBrowse() != t1.getBrowse() && t1.getBrowse() == 1) {
                                threadEventEntity.setBrowse(1);
                            }
                            if (threadEventEntity.getDetail() != t1.getDetail() && t1.getDetail() == 1) {
                                threadEventEntity.setDetail(1);
                            }
                            if (threadEventEntity.getLike() != t1.getLike() && t1.getLike() == 1) {
                                threadEventEntity.setLike(1);
                            }
                            if (threadEventEntity.getComment() != t1.getComment() && t1.getComment() == 1) {
                                threadEventEntity.setComment(1);
                            }
                            if (threadEventEntity.getCollect() != t1.getCollect() && t1.getCollect() == 1) {
                                threadEventEntity.setCollect(1);
                            }
                        }
                        return threadEventEntity;
                    }
                })
                .flatMap(new FlatMapFunction <ThreadEventEntity, ThreadEventEntity>() {
                    @Override
                    public void flatMap(ThreadEventEntity threadEventEntity, Collector <ThreadEventEntity> collector) throws Exception {
                        setThreadVector(threadEventEntity);
                        setUserVector(threadEventEntity);
                        collector.collect(threadEventEntity);
                    }
                })
                .addSink(new MyTiDbSink <>())
        ;
    }

    private static void browse(BaseInfoEntity baseInfoEntity, Collector <ThreadEventEntity> collector) throws Exception {
        List <ThreadEventEntity> list = new ArrayList <>();
        List <String> itemList = new ArrayList <>();
        String properties = baseInfoEntity.getItemId().toString();
        try {
            itemList = JSON.parseObject(properties, List.class);
        } catch (Exception e) {
            return;
        }
        if (itemList.isEmpty()) {
            try {
                Map <String, String> map = JSONObject.parseObject(baseInfoEntity.getItemId().toString(), Map.class);
                for (Map.Entry <String, String> entry : map.entrySet()) {
                    itemList.add(entry.getKey());
                }
            } catch (Exception e) {
                return;
            }
        }
//        Map <String, ThreadVectorEntity> map = getThreadVector(itemList);
        for (String s : itemList) {
            ThreadEventEntity threadEventEntity = new ThreadEventEntity();
            threadEventEntity.setBrowse(1);
            threadEventEntity.setUser_id(baseInfoEntity.getUserId());
            threadEventEntity.setThread_id(s);
            threadEventEntity.setTimestamp(new Date().getTime());
//            if (map != null && map.containsKey(s)) {
//                setThreadVector(map.get(s), threadEventEntity);
//            }
            String base = baseInfoEntity.getUserId() + s;
            threadEventEntity.setId(DigestUtils.md5Hex(base));
//            setUserVector(baseInfoEntity.getUserId(), threadEventEntity);
            collector.collect(threadEventEntity);
        }
    }

    private static void common(BaseInfoEntity baseInfoEntity, Collector <ThreadEventEntity> collector, EventEnums eventEnums) throws Exception {
        ThreadEventEntity threadEventEntity = new ThreadEventEntity();
        threadEventEntity.setUser_id(baseInfoEntity.getUserId());
        threadEventEntity.setThread_id(baseInfoEntity.getItemId().toString());
        String base = baseInfoEntity.getUserId() + baseInfoEntity.getItemId().toString();
        threadEventEntity.setId(DigestUtils.md5Hex(base));
        threadEventEntity.setType(1);
        threadEventEntity.setTimestamp(new Date().getTime());
        switch (eventEnums) {
            case Detail:
                threadEventEntity.setDetail(1);
                break;
            case Like:
                threadEventEntity.setLike(1);
                break;
            case Share:
                threadEventEntity.setShare(1);
                break;
            case Comment:
                threadEventEntity.setComment(1);
                break;
            case Collect:
                threadEventEntity.setCollect(1);
                break;
            default:
                return;
        }
//        ThreadVectorEntity threadVector = getThreadVector(baseInfoEntity.getItemId().toString());
//        setThreadVector(threadVector, threadEventEntity);
//        setUserVector(baseInfoEntity.getUserId(), threadEventEntity);
        collector.collect(threadEventEntity);
    }

    private static void setUserVector(ThreadEventEntity threadEventEntity) throws Exception {
        UserVectorEntity userVectorEntity = getUserVector(threadEventEntity.getUser_id());
        if (userVectorEntity != null) {
            if (userVectorEntity.getAction_time_vector() != null) {
                threadEventEntity.setAction_time_vector(userVectorEntity.getAction_time_vector());
            }
            if (userVectorEntity.getCarrier_vector() != null) {
                threadEventEntity.setCarrier_vector(userVectorEntity.getCarrier_vector());
            }
            if (userVectorEntity.getCity_vector() != null) {
                threadEventEntity.setCity_vector(userVectorEntity.getCity_vector());
            }
            if (userVectorEntity.getManufacturer_vector() != null) {
                threadEventEntity.setManufacturer_vector(userVectorEntity.getManufacturer_vector());
            }
            if (userVectorEntity.getNetwork_type_vector() != null) {
                threadEventEntity.setNet_type_vector(userVectorEntity.getNetwork_type_vector());
            }
            if (userVectorEntity.getOs_vector() != null) {
                threadEventEntity.setOs_vector(userVectorEntity.getOs_vector());
            }
            if (userVectorEntity.getScreen_height_vector() != null) {
                threadEventEntity.setScreen_height_vector(userVectorEntity.getScreen_height_vector());
            }
            if (userVectorEntity.getScreen_width_vector() != null) {
                threadEventEntity.setScreen_width_vector(userVectorEntity.getScreen_width_vector());
            }
            if (userVectorEntity.getStaff_vector() != null) {
                threadEventEntity.setStaff_vector(userVectorEntity.getStaff_vector());
            }
            if (userVectorEntity.getBirthday_vector() != null) {
                threadEventEntity.setBirthday_vector(userVectorEntity.getBirthday_vector());
            }
            if (userVectorEntity.getIs_vip_vector() != null) {
                threadEventEntity.setIs_vip_vector(userVectorEntity.getIs_vip_vector());
            }
            if (userVectorEntity.getRegister_time_vector() != null) {
                threadEventEntity.setRegister_time_vector(userVectorEntity.getRegister_time_vector());
            }
            if (userVectorEntity.getSex_vector() != null) {
                threadEventEntity.setSex_vector(userVectorEntity.getSex_vector());
            }
            if (userVectorEntity.getSun_vector() != null) {
                threadEventEntity.setSun_vector(userVectorEntity.getSun_vector());
            }
            if (userVectorEntity.getPay_vector() != null) {
                threadEventEntity.setPay_vector(userVectorEntity.getPay_vector());
            }
        }
    }

    private static void setThreadVector(ThreadEventEntity threadEventEntity) throws Exception {
        ThreadVectorEntity threadVectorEntity = getThreadVector(threadEventEntity.getThread_id());
        if (threadVectorEntity != null) {
            if (threadVectorEntity.getContent_vector() != null) {
                threadEventEntity.setContent_vector(JSON.toJSONString(threadVectorEntity.getContent_vector()));
            }
            if (threadVectorEntity.getCreate_time_vector() != null) {
                threadEventEntity.setCreate_time_vector(threadVectorEntity.getCreate_time_vector());
            }
            if (threadVectorEntity.getImage_score_vector() != null) {
                threadEventEntity.setImage_score_vector(threadVectorEntity.getImage_score_vector());
            }
            if (threadVectorEntity.getIs_selfie_vector() != null) {
                threadEventEntity.setIs_selfie_vector(threadVectorEntity.getIs_selfie_vector());
            }
            if (threadVectorEntity.getLike_vector() != null) {
                threadEventEntity.setLike_vector(threadVectorEntity.getLike_vector());
            }
            if (threadVectorEntity.getWord_count_vector() != null) {
                threadEventEntity.setWord_count_vector(threadVectorEntity.getWord_count_vector());
            }
            if (threadVectorEntity.getComment_vector() != null) {
                threadEventEntity.setComment_vector(threadVectorEntity.getComment_vector());
            }
        }
    }

    private static UserVectorEntity getUserVector(String userId) throws Exception {
        UserVectorEntity userVectorEntity = new UserVectorEntity();
        String sql = "select * from user_vector where id='" + userId + "'";
        Connection connection = null;
        try {
            connection = MysqlClient.getConnect();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            boolean next = resultSet.next();
            if (next) {
                userVectorEntity.setId(resultSet.getString("id"));
                if (resultSet.getString("birthday_vector") != null && !resultSet.getString("birthday_vector").equals("null")) {
                    userVectorEntity.setBirthday_vector(resultSet.getString("birthday_vector"));
                }

                if (resultSet.getString("sex_vector") != null && !resultSet.getString("sex_vector").equals("null")) {
                    userVectorEntity.setSex_vector(resultSet.getString("sex_vector"));
                }

                if (resultSet.getString("sun_vector") != null && !resultSet.getString("sun_vector").equals("null")) {
                    userVectorEntity.setSun_vector(resultSet.getString("sun_vector"));
                }

                if (resultSet.getString("is_vip_vector") != null && !resultSet.getString("is_vip_vector").equals("null")) {
                    userVectorEntity.setIs_vip_vector(resultSet.getString("is_vip_vector"));
                }
                if (resultSet.getString("register_time_vector") != null && !resultSet.getString("register_time_vector").equals("null")) {
                    userVectorEntity.setRegister_time_vector(resultSet.getString("register_time_vector"));
                }
                if (resultSet.getString("action_time_vector") != null && !resultSet.getString("action_time_vector").equals("null")) {
                    userVectorEntity.setAction_time_vector(resultSet.getString("action_time_vector"));
                }
                if (resultSet.getString("city_vector") != null && !resultSet.getString("city_vector").equals("null")) {
                    userVectorEntity.setCity_vector(resultSet.getString("city_vector"));
                }
                if (resultSet.getString("manufacturer_vector") != null && !resultSet.getString("manufacturer_vector").equals("null")) {
                    userVectorEntity.setManufacturer_vector(resultSet.getString("manufacturer_vector"));
                }
                if (resultSet.getString("os_vector") != null && !resultSet.getString("os_vector").equals("null")) {
                    userVectorEntity.setOs_vector(resultSet.getString("os_vector"));
                }
                if (resultSet.getString("screen_height_vector") != null && !resultSet.getString("screen_height_vector").equals("null")) {
                    userVectorEntity.setScreen_height_vector(resultSet.getString("screen_height_vector"));
                }
                if (resultSet.getString("screen_width_vector") != null && !resultSet.getString("screen_width_vector").equals("null")) {
                    userVectorEntity.setScreen_width_vector(resultSet.getString("screen_width_vector"));
                }
                if (resultSet.getString("network_type_vector") != null && !resultSet.getString("network_type_vector").equals("null")) {
                    userVectorEntity.setNetwork_type_vector(resultSet.getString("network_type_vector"));
                }
                if (resultSet.getString("carrier_vector") != null && !resultSet.getString("carrier_vector").equals("null")) {
                    userVectorEntity.setCarrier_vector(resultSet.getString("carrier_vector"));
                }
                if (resultSet.getString("staff_vector") != null && !resultSet.getString("staff_vector").equals("null")) {
                    userVectorEntity.setStaff_vector(resultSet.getString("staff_vector"));
                }
                if (resultSet.getString("pay_vector") != null && !resultSet.getString("pay_vector").equals("null")) {
                    userVectorEntity.setPay_vector(resultSet.getString("pay_vector"));
                }
            }
            preparedStatement.close();
        } catch (Exception e) {
            log.error("TD sql error : {}", e.getMessage());
        } finally {
            MysqlClient.close(connection);
        }
        return userVectorEntity;
    }

    private static ThreadVectorEntity getThreadVector(String threadId) throws Exception {
        ThreadVectorEntity threadVectorEntity = new ThreadVectorEntity();
        String sql = "select * from thread_vector where id='" + threadId + "'";
        Connection connection = null;
        try {
            connection = MysqlClient.getConnect();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            boolean next = resultSet.next();
            if (next) {
                threadVectorEntity.setId(resultSet.getString("id"));
                if (resultSet.getString("content_vector") != null && !resultSet.getString("content_vector").equals("null")) {
                    threadVectorEntity.setContent_vector(resultSet.getString("content_vector"));
                }
                if (resultSet.getString("like_vector") != null && !resultSet.getString("like_vector").equals("null")) {
                    threadVectorEntity.setLike_vector(resultSet.getString("like_vector"));
                }
                if (resultSet.getString("comment_vector") != null && !resultSet.getString("comment_vector").equals("null")) {
                    threadVectorEntity.setComment_vector(resultSet.getString("comment_vector"));
                }
                if (resultSet.getString("create_time_vector") != null && !resultSet.getString("create_time_vector").equals("null")) {
                    threadVectorEntity.setCreate_time_vector(resultSet.getString("create_time_vector"));
                }
                if (resultSet.getString("word_count_vector") != null && !resultSet.getString("word_count_vector").equals("null")) {
                    threadVectorEntity.setWord_count_vector(resultSet.getString("word_count_vector"));
                }
                if (resultSet.getString("image_score_vector") != null && !resultSet.getString("image_score_vector").equals("null")) {
                    threadVectorEntity.setImage_score_vector(resultSet.getString("image_score_vector"));
                }
                if (resultSet.getString("is_selfie_vector") != null && !resultSet.getString("is_selfie_vector").equals("null")) {
                    threadVectorEntity.setIs_selfie_vector(resultSet.getString("is_selfie_vector"));
                }
                if (resultSet.getString("is_selfie_vector") != null && !resultSet.getString("is_selfie_vector").equals("null")) {
                    threadVectorEntity.setIs_selfie_vector(resultSet.getString("is_selfie_vector"));
                }
                if (resultSet.getString("topic_vector") != null && !resultSet.getString("topic_vector").equals("null")) {
                    threadVectorEntity.setTopic_vector(resultSet.getString("topic_vector"));
                }
            }
            preparedStatement.close();
        } catch (Exception e) {
            log.error("TD sql error : {}", e.getMessage());
        } finally {
            MysqlClient.close(connection);
        }
        return threadVectorEntity;
    }

    private static Map <String, ThreadVectorEntity> getThreadVector(List <String> threadId) {
        Map <String, ThreadVectorEntity> map = new HashMap <>();
        BasicDBObject condition = new BasicDBObject();
        MongoCollection <Document> collection = MongodbUtils.getCollection("thread", "cece_thread_vector");
        condition.put("_id", new BasicDBObject("$in", threadId));
        List <?> list = MongodbUtils.getByCondition(collection, condition, ThreadVectorEntity.class);
        if (list.isEmpty()) {
            return null;
        }
        for (Object o : list) {
            if (o == null) {
                continue;
            }
            ThreadVectorEntity threadVectorEntity = (ThreadVectorEntity) o;
            map.put(threadVectorEntity.getId(), threadVectorEntity);
        }
        return map;
    }


}
