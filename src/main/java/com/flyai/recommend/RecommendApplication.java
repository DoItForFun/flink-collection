package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.classification.DataClassification;
import com.flyai.recommend.entity.*;
import com.flyai.recommend.mapper.MyRedisMapper;
import com.flyai.recommend.mapper.MyRedisSink2;
import com.flyai.recommend.utils.KafkaPropertiesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class RecommendApplication {
    private static StreamExecutionEnvironment env;
    private static final OutputTag <ThreadActionEntity> outputTag1 = new OutputTag <ThreadActionEntity>("stream1") {};

    public static void main(String[] args) {
        try {
            setEnv();
            StreamExecutionEnvironment streamExecutionEnvironment = setCheckPoint(env);
            DataStream <BaseInfoEntity> dataStream = eventThreadStreamProcess(streamExecutionEnvironment);
            async(dataStream);
            env.execute("data classification job start");
        } catch (Exception e) {
            log.error("job run exception:{}", Throwables.getStackTraceAsString(e));
        }
    }

    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static StreamExecutionEnvironment setCheckPoint(StreamExecutionEnvironment env) {
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    private static DataStream <String> getStreamDataSource(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.load(RecommendApplication.class.getResourceAsStream("/application.properties"));
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"));
        kafkaProperties.setProperty("group.id", properties.getProperty("kafka.event.consumer.group"));
        kafkaProperties.setProperty("enable.auto.commit", "false");
        String topics = properties.getProperty("kafka.event.consumer.topic");
        FlinkKafkaConsumer <String> consumer = new FlinkKafkaConsumer <String>(topics, new SimpleStringSchema(), kafkaProperties);
        consumer.setStartFromGroupOffsets();
        return env.addSource(consumer);
    }

    private static DataStream <BaseInfoEntity> eventThreadStreamProcess(StreamExecutionEnvironment streamExecutionEnvironment) throws Exception {
        return getStreamDataSource(streamExecutionEnvironment)
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
                });

    }

    private static void async(DataStream <BaseInfoEntity> dataStream) throws Exception {
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("10.19.52.120")
                .setMaxTotal(5000)
                .setMaxIdle(10)
                .setMinIdle(5)
                .setPort(6379)
                .setPassword("cece_xxwolo")
                .build();
        SingleOutputStreamOperator<ReduceActionEntity> stream = dataStream
                .flatMap(new FlatMapFunction <BaseInfoEntity, List <ThreadActionEntity>>() {
                    @Override
                    public void flatMap(BaseInfoEntity baseInfoEntity, Collector <List <ThreadActionEntity>> collector) throws Exception {
                        collector.collect(DataClassification.classificationData(baseInfoEntity));
                    }
                })
                .flatMap(new FlatMapFunction <List <ThreadActionEntity>, ReduceActionEntity>() {
                    @Override
                    public void flatMap(List <ThreadActionEntity> threadActionEntities, Collector <ReduceActionEntity> collector) throws Exception {
                        if (threadActionEntities == null) {
                            return;
                        }
                        for (ThreadActionEntity threadActionEntity : threadActionEntities) {
                            if (threadActionEntity == null) {
                                continue;
                            }
                            ReduceActionEntity reduceActionEntity = new ReduceActionEntity();
                            reduceActionEntity.setThreadId(threadActionEntity.getThreadId());
                            switch (threadActionEntity.getAction()){
                                case "Browse":
                                    reduceActionEntity.setBrowse(1);
                                    break;
                                case "Browsed":
                                    reduceActionEntity.setBrowsed(1);
                                    break;
                                case "Detail":
                                    reduceActionEntity.setDetail(1);
                                    break;
                                case "PublishSave":
                                    reduceActionEntity.setPublishSave(1);
                                    break;
                                case "Publish":
                                    reduceActionEntity.setPublish(1);
                                    break;
                                case "Comment":
                                    reduceActionEntity.setComment(1);

                                    break;
                                case "Share":
                                    reduceActionEntity.setShare(1);

                                    break;
                                case "Like":
                                    reduceActionEntity.setLike(1);
                                    break;
                                default:
                                    return;
                            }
                            collector.collect(reduceActionEntity);
                        }
                    }
                })
                .keyBy("threadId")
                .timeWindow(Time.seconds(30))
                .reduce(new ReduceFunction <ReduceActionEntity>() {
                    @Override
                    public ReduceActionEntity reduce(ReduceActionEntity reduceActionEntity, ReduceActionEntity t1) throws Exception {
                        ReduceActionEntity total = reduceActionEntity;
                        if(t1 != null && reduceActionEntity.getThreadId().equals(t1.getThreadId())){
                            if(t1.getBrowse() != null){
                                int browse = total.getBrowse() == null ? 0 : total.getBrowse();
                                total.setBrowse(browse + t1.getBrowse());
                            }
                            if(t1.getBrowsed() != null){
                                int browsed = total.getBrowsed() == null ? 0 : total.getBrowsed();
                                total.setBrowsed(browsed + t1.getBrowsed());
                            }
                            if(t1.getComment() != null){
                                int comment = total.getComment() == null ? 0 : total.getComment();
                                total.setComment(comment + t1.getComment());
                            }
                            if(t1.getDetail() != null){
                                int detail = total.getDetail() == null ? 0 : total.getDetail();
                                total.setDetail(detail + t1.getDetail());
                            }
                            if(t1.getPublishSave() != null){
                                int publishSave = total.getPublishSave() == null ? 0 : total.getPublishSave();
                                total.setPublishSave(publishSave + t1.getPublishSave());
                            }
                            if(t1.getPublish() != null){
                                int publish = total.getPublish() == null ? 0 : total.getPublish();
                                total.setPublish(publish + t1.getPublish());
                            }
                            if(t1.getShare() != null){
                                int share = total.getShare() == null ? 0 : total.getShare();
                                total.setShare(share + t1.getShare());
                            }
                            if(t1.getLike() != null){
                                int like = total.getLike() == null ? 0 : total.getLike();
                                total.setLike(like + t1.getLike());
                            }
                        }
                        return total;
                    }
                })
                .process(new ProcessFunction <ReduceActionEntity, ReduceActionEntity>() {
                    @Override
                    public void processElement(ReduceActionEntity reduceActionEntity, Context context, Collector <ReduceActionEntity> collector) throws Exception {
                        if(reduceActionEntity.getDetail() != null
                                || reduceActionEntity.getShare() != null
                                || reduceActionEntity.getComment() != null
                                || reduceActionEntity.getLike() != null
                                || reduceActionEntity.getPublish() != null
                                || reduceActionEntity.getPublishSave() != null
                                || reduceActionEntity.getBrowsed() != null
                        ){
                            ThreadActionEntity threadActionEntity = new ThreadActionEntity();
                            threadActionEntity.setThreadId(reduceActionEntity.getThreadId());
                            context.output(outputTag1 , threadActionEntity);
                        }
                        collector.collect(reduceActionEntity);
                    }
                });
        FlinkKafkaProducer <String> producer = new KafkaPropertiesUtils("kafka.bootstrap.servers", "kafka.event.producer.group", "kafka.event.producer.topic").getKafkaProducer();
        DataStream<ThreadActionEntity> returnData = stream.getSideOutput(outputTag1);
        returnData.flatMap(new FlatMapFunction <ThreadActionEntity, String>() {
            @Override
            public void flatMap(ThreadActionEntity o, Collector <String> collector) throws Exception {
                if (o != null) {
                    collector.collect(JSON.toJSONString(o));
                }
            }
        })
                .addSink(producer)
                .name("data classification");
        stream.addSink(new MyRedisSink2 <>(config , new MyRedisMapper <>()));
    }
}
