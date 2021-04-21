package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.entity.ThreadVectorActionEntity;
import com.flyai.recommend.entity.ThreadVectorEntity;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.mapper.MyTiDbSink;
import com.flyai.recommend.utils.MongodbUtils;
import com.flyai.recommend.utils.VectorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class VectorApplication {
    private static StreamExecutionEnvironment env;
    private static DataStream <String> dataStream;
    private static List<Integer> processList = new ArrayList<>();
    static {
        processList.add(EventEnums.Add.value());
        processList.add(EventEnums.Like.value());
        processList.add(EventEnums.Comment.value());
        processList.add(EventEnums.ImageScore.value());
    }


    public static void main(String[] args) {
        try {
            setEnv();
            setCheckPoint();
            setSource();
            vectorCalculate();
            env.execute("Vector job start");
        } catch (Exception e) {
            log.error("job run exception:{}", Throwables.getStackTraceAsString(e));
        }
    }


    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void setCheckPoint() {
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    private static void setSource() throws IOException {
        Properties properties = new Properties();
        properties.load(VectorApplication.class.getResourceAsStream("/application.properties"));
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"));
        kafkaProperties.setProperty("group.id", properties.getProperty("kafka.event.consumer.vector.group"));
        kafkaProperties.setProperty("enable.auto.commit", "false");
        String topics = properties.getProperty("kafka.event.consumer.topic");
        FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <String>(topics, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();
        dataStream = env.addSource(consumer);
    }


    private static void vectorCalculate() {
        dataStream
                .filter(new FilterFunction <String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (s == null || s.isEmpty()) {
                    return false;
                }
                return true;
            }})
                .map(new MapFunction <String, BaseInfoEntity>() {
                    @Override
                    public BaseInfoEntity map(String s) throws Exception {
                        Map map = JSON.parseObject(s, Map.class);
                        if (!map.containsKey("properties")) {
                            return null;
                        }
                        Map data = JSON.parseObject(map.get("properties").toString(), Map.class);
                        if (!data.containsKey("properties")) {
                            return null;
                        }
                        return JSON.parseObject(data.get("properties").toString(), BaseInfoEntity.class);
                    }
                })
                .flatMap(new FlatMapFunction <BaseInfoEntity, ThreadVectorActionEntity>() {
                    @Override
                    public void flatMap(BaseInfoEntity baseInfoEntity, Collector <ThreadVectorActionEntity> collector) throws Exception {
                        if(baseInfoEntity == null
                                || baseInfoEntity.getActionId() == null
                                || !processList.contains(Integer.parseInt(baseInfoEntity.getActionId()))
                        ){
                            return;
                        }
                        ThreadVectorActionEntity threadVectorActionEntity = new ThreadVectorActionEntity();
                        threadVectorActionEntity.setThreadId(baseInfoEntity.getItemId().toString());
                        switch (Integer.parseInt(baseInfoEntity.getActionId())){
                            case 0:
                                threadVectorActionEntity.setCreate(1);
                                break;
                            case 5:
                                threadVectorActionEntity.setLike(1);
                                break;
                            case 6:
                                threadVectorActionEntity.setComment(1);
                                break;
                            case 13:
                                threadVectorActionEntity.setImageScore(1);
                                break;
                            default:
                                return;
                        }
                        collector.collect(threadVectorActionEntity);
                    }
                })
                .keyBy("threadId")
                .timeWindow(Time.minutes(5))
                .reduce(new ReduceFunction <ThreadVectorActionEntity>() {
                    @Override
                    public ThreadVectorActionEntity reduce(ThreadVectorActionEntity threadVectorActionEntity, ThreadVectorActionEntity t1) throws Exception {
                        ThreadVectorActionEntity total = threadVectorActionEntity;
                        if(t1 != null
                                && t1.getThreadId() != null
                                && threadVectorActionEntity != null
                                && threadVectorActionEntity.getThreadId() != null
                                && threadVectorActionEntity.getThreadId().equals(t1.getThreadId()))
                        {
                            if(t1.getCreate() != null){
                                int create = total.getCreate() == null ? 0 : total.getCreate();
                                total.setCreate(create + t1.getCreate());
                            }
                            if(t1.getComment() != null){
                                int comment = total.getComment() == null ? 0 : total.getComment();
                                total.setComment(comment + t1.getComment());
                            }
                            if(t1.getImageScore() != null){
                                int imageScore = total.getImageScore() == null ? 0 : total.getImageScore();
                                total.setImageScore(imageScore + t1.getImageScore());
                            }
                            if(t1.getLike() != null){
                                int like = total.getLike() == null ? 0 : total.getLike();
                                total.setLike(like + t1.getLike());
                            }
                            if(t1.getTopic() != null){
                                int topic = total.getTopic() == null ? 0 : total.getTopic();
                                total.setLike(topic + t1.getTopic());
                            }
                        }
                        return total;
                    }
                })
                .flatMap(new FlatMapFunction <ThreadVectorActionEntity, ThreadVectorEntity>() {
                    @Override
                    public void flatMap(ThreadVectorActionEntity threadVectorActionEntity, Collector <ThreadVectorEntity> collector) throws Exception {
                        if(threadVectorActionEntity == null){
                            return;
                        }
                        // @todo mongodb 查询帖子信息
                        BasicDBObject condition = MongodbUtils.getCondition("threadId", threadVectorActionEntity.getThreadId());
                        MongoCollection <Document> collection = MongodbUtils.getCollection("thread", "cece_thread");
                        ThreadEntity entity = (ThreadEntity) MongodbUtils.getOneByCondition(collection , condition , ThreadEntity.class);

                        MongoCollection <Document> vectorCollection = MongodbUtils.getCollection("thread", "cece_thread_vector");
                        BasicDBObject vectorCondition = MongodbUtils.getCondition("_id", threadVectorActionEntity.getThreadId());
                        ThreadVectorEntity checkThreadVectorEntity = (ThreadVectorEntity) MongodbUtils.getOneByCondition(vectorCollection , vectorCondition , ThreadVectorEntity.class);
                        if(entity == null){
                            return;
                        }

                        ThreadVectorEntity threadVectorEntity = new ThreadVectorEntity();
                        threadVectorEntity.setId(entity.getThreadId());
                        if(threadVectorActionEntity.getCreate() != null
                            || (checkThreadVectorEntity != null && checkThreadVectorEntity.getContent_vector() == null)
                        ){
                            if(entity.getContent() != null && !entity.getContent().isEmpty()){
                                Map<String, String> map = new HashMap <>();
                                map.put("texts" , entity.getContent());
                                map.put("userId" , entity.getAuthorId());
                                List<String> vecFromApi = VectorUtils.getVecFromApi(map);
                                threadVectorEntity.setContent_vector(JSON.toJSONString(vecFromApi));
                                int length = entity.getContent().length();
                                // @todo 字数分组
                                String wordCountVector = VectorUtils.wordCount(length);
                                if(wordCountVector != null){
                                    threadVectorEntity.setWord_count_vector(wordCountVector);
                                }
                            }
                            // @todo 星期分组
                            String weekVector = VectorUtils.week(entity.getCreateTime().toString());
                            if(weekVector != null){
                                threadVectorEntity.setCreate_time_vector(weekVector);
                            }
                        }
                        if(threadVectorActionEntity.getLike() != null
                            || (checkThreadVectorEntity != null && checkThreadVectorEntity.getLike_vector() == null)
                        ){
                            // @todo 点赞分组
                            String likeVector = VectorUtils.likeOrComment(entity.getLikeCount());
                            if(likeVector != null){
                                threadVectorEntity.setLike_vector(likeVector);
                            }
                        }
                        if(threadVectorActionEntity.getComment() != null){
                            String commentVector = VectorUtils.likeOrComment(entity.getLikeCount());
                            if(commentVector != null){
                                threadVectorEntity.setComment_vector(commentVector);
                            }
                        }
                        if(threadVectorActionEntity.getImageScore() != null
                                || (checkThreadVectorEntity != null && checkThreadVectorEntity.getImage_score_vector() == null)
                        ){
                            // @todo 图片质量分类
                            String scoreVector = VectorUtils.score(entity.getScore());
                            if(scoreVector != null){
                                threadVectorEntity.setImage_score_vector(scoreVector);
                            }
                            // @todo 是否为自拍分类
                            String selfieVector = VectorUtils.isSelfie(entity.getIsSelfie());
                            if(selfieVector != null){
                                threadVectorEntity.setIs_selfie_vector(selfieVector);
                            }
                        }
                        collector.collect(threadVectorEntity);
                    }
                })
                .addSink(new MyTiDbSink <>());
    }

}
