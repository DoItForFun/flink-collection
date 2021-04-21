package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.RedisActionEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.enums.AuthorEnums;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.enums.RedisActionEnums;
import com.flyai.recommend.handler.GetThreadHandler;
import com.flyai.recommend.mapper.MyRedisMapper;
import com.flyai.recommend.mapper.MyRedisSink;
import com.flyai.recommend.service.RecommendAuthorDataService;
import com.flyai.recommend.service.RecommendThreadDataService;
import com.flyai.recommend.utils.KafkaPropertiesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class ReCommendCalculateApplication {
    private static StreamExecutionEnvironment env;

    public static void main(String[] args) {
        try {
            setEnv();
            StreamExecutionEnvironment streamExecutionEnvironment = setCheckPoint(env);
            DataStream <RedisActionEntity> dataStream = process(streamExecutionEnvironment);
            addToRedis(dataStream);
            env.execute("data calculate job start");
        } catch (Exception e) {
            log.error("job run exception:{}", Throwables.getStackTraceAsString(e));
        }

    }

    private static void setEnv() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static StreamExecutionEnvironment setCheckPoint(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }

    private static DataStream <RedisActionEntity> process(StreamExecutionEnvironment streamExecutionEnvironment) throws Exception {
        FlinkKafkaConsumer <String> consumer = new KafkaPropertiesUtils("kafka.bootstrap.servers", "kafka.event.producer.group", "kafka.event.producer.topic").getKafkaConsumer();
        consumer.setStartFromGroupOffsets();
        return streamExecutionEnvironment.addSource(consumer)
                .filter((FilterFunction <String>) s -> s != null && !s.isEmpty())
                .map(new MapFunction <String, ThreadActionEntity>() {
                    @Override
                    public ThreadActionEntity map(String s) throws Exception {
                        try {
                            return JSON.parseObject(s, ThreadActionEntity.class);
                        } catch (Exception e) {
                            return null;
                        }
                    }
                })
                .filter((FilterFunction <ThreadActionEntity>) Objects::nonNull)
                .flatMap(new FlatMapFunction <ThreadActionEntity, RedisActionEntity>() {
                    @Override
                    public void flatMap(ThreadActionEntity threadActionEntity, Collector <RedisActionEntity> collector) throws Exception {
                        threadCalculate(threadActionEntity.getThreadId(), collector);
                    }
                });
    }

    private static void threadCalculate(String threadId, Collector <RedisActionEntity> collector) throws InterruptedException {
        Map <String, String> threadRecommendData = RecommendThreadDataService.threadHGetAll(threadId);
        if (threadRecommendData == null
                || threadRecommendData.isEmpty()
        ) {
            return;
        }
        ThreadEntity threadEntity = GetThreadHandler.process(threadId);
        if (threadEntity == null) {
            return;
        }
        String likeKey = EventEnums.Like.desc();
        String commentKey = EventEnums.Comment.desc();
        String detailKey = EventEnums.Detail.desc();
        String browseKey = EventEnums.Browse.desc();
        String shareKey = EventEnums.Share.desc();
        String collectionKey = EventEnums.Collect.desc();

        BigDecimal browse = BigDecimal.valueOf(Double.parseDouble(threadRecommendData.getOrDefault(browseKey, "0")));
        BigDecimal detail = BigDecimal.valueOf(Double.parseDouble(threadRecommendData.getOrDefault(detailKey, "0")));
        BigDecimal share = BigDecimal.valueOf(Double.parseDouble(threadRecommendData.getOrDefault(shareKey, "0")));
        BigDecimal collection = BigDecimal.valueOf(Double.parseDouble(threadRecommendData.getOrDefault(collectionKey, "0")));
        BigDecimal like = BigDecimal.valueOf(Long.parseLong(threadRecommendData.getOrDefault(likeKey, "0")));
        BigDecimal comment = BigDecimal.valueOf(Long.parseLong(threadRecommendData.getOrDefault(commentKey, "0")));
        BigDecimal createTime = BigDecimal.valueOf(threadEntity.getCreateTime() == null ? 0L : threadEntity.getCreateTime());
        BigDecimal contentExponent = new BigDecimal(0);
        BigDecimal contentPoint = new BigDecimal(0);
        BigDecimal contentPointDetailPart = new BigDecimal(0);
        BigDecimal contentPointCollectionPart = new BigDecimal(0);
        BigDecimal contentPointSharePart = new BigDecimal(0);
        BigDecimal interactivePoint = new BigDecimal(0);
        BigDecimal interactivePointLikePart = new BigDecimal(0);
        BigDecimal interactivePointCommentPart = new BigDecimal(0);
        BigDecimal detailPoint = new BigDecimal(0);
        BigDecimal likePoint = new BigDecimal(0);
        BigDecimal commentPoint = new BigDecimal(0);
        BigDecimal timePoint = new BigDecimal(0);
        BigDecimal exPoint = new BigDecimal(0);
        if (detail.compareTo(new BigDecimal(0)) > 0) {
            BigDecimal n = new BigDecimal(100);
            exPoint = detail.divide(browse.add(n) , 5).multiply(n);
            detailPoint = detail.divide(new BigDecimal(50000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.3));
            if (detail.compareTo(new BigDecimal(50000)) > 0) {
                detail = BigDecimal.valueOf(50000);
            }
            contentPointDetailPart = detail.divide(BigDecimal.valueOf(50000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.7));
        }

        if(collection.compareTo(new BigDecimal(0)) > 0){
            if (collection.compareTo(new BigDecimal(1000)) > 0) {
                collection = BigDecimal.valueOf(1000);
            }
            contentPointCollectionPart = collection.divide(BigDecimal.valueOf(1000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.3));
        }
        if (share.compareTo(new BigDecimal(0)) > 0) {
            if (share.compareTo(new BigDecimal(500)) > 0) {
                share = BigDecimal.valueOf(500);
            }
            contentPointSharePart = share.divide(BigDecimal.valueOf(500), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.2));
        }
        if (like.compareTo(new BigDecimal(0)) > 0) {
            likePoint = like.divide(BigDecimal.valueOf(2000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.1));
            if (like.compareTo(new BigDecimal(2000)) > 0) {
                like = BigDecimal.valueOf(2000);
            }
            interactivePointLikePart = like.divide(BigDecimal.valueOf(2000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.3));
        }

        if (comment.compareTo(new BigDecimal(0)) > 0) {
            commentPoint = comment.divide(BigDecimal.valueOf(10000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.4));
            if (comment.compareTo(new BigDecimal(10000)) > 0) {
                comment = BigDecimal.valueOf(10000);
            }
            interactivePointCommentPart = comment.divide(BigDecimal.valueOf(10000), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.5));
        }
        if (createTime.compareTo(new BigDecimal(0)) > 0) {
            double now = new Date().getTime();
            timePoint = createTime.divide(BigDecimal.valueOf(now), 5, BigDecimal.ROUND_HALF_UP).multiply(BigDecimal.valueOf(0.2));
        }
        detailPoint = detailPoint.add(likePoint).add(commentPoint).add(timePoint);
        contentExponent = detailPoint.multiply(BigDecimal.valueOf(1000));
        if(exPoint.compareTo(new BigDecimal(0)) > 0){
            contentExponent = contentPoint.multiply(exPoint);
        }
        String key = "recommendData:threadId:" + threadId;
        RedisActionEntity contentExponentAction = new RedisActionEntity(RedisActionEnums.HSet.value(), key, "contentExponent", contentExponent.toPlainString());
        collector.collect(contentExponentAction);
        contentPoint = contentPointDetailPart.add(contentPointCollectionPart);
        RedisActionEntity contentPointAction = new RedisActionEntity(RedisActionEnums.HSet.value(), key, "contentPoint", contentPoint.toPlainString());
        collector.collect(contentPointAction);
        interactivePoint = interactivePointLikePart.add(interactivePointCommentPart).add(contentPointSharePart);
        RedisActionEntity interactivePointAction = new RedisActionEntity(RedisActionEnums.HSet.value(), key, "interactivePoint", interactivePoint.toPlainString());
        collector.collect(interactivePointAction);
        RedisActionEntity updateList = new RedisActionEntity(RedisActionEnums.LPush.value(), null, "recommendData:updateThreadList", threadId);
        collector.collect(updateList);

        String authorId = threadEntity.getAuthorId();
        Map <String, String> authorData = RecommendAuthorDataService.getAuthorData(authorId);
        BigDecimal published = BigDecimal.valueOf(authorData != null && authorData.containsKey(AuthorEnums.Publish.desc()) ? Double.parseDouble(authorData.get(AuthorEnums.Publish.desc())) : 0);
        published = published.compareTo(new BigDecimal(200)) > 0 ? new BigDecimal(200) : published;
        BigDecimal publishedPoint = published.divide(new BigDecimal(200), 5, BigDecimal.ROUND_HALF_UP);
        BigDecimal browsed = BigDecimal.valueOf(authorData != null && authorData.containsKey(AuthorEnums.Browse.desc()) ? Double.parseDouble(authorData.get(AuthorEnums.Browse.desc())) : 0);
        browsed = browsed.compareTo(new BigDecimal(10000000)) > 0 ? new BigDecimal(10000000) : browsed;
        BigDecimal browsedPoint = browsed.divide(new BigDecimal(10000000), 5, BigDecimal.ROUND_HALF_UP);

        Set <String> threadSet = RecommendAuthorDataService.getPublish(authorId);
        if (threadSet != null) {
            BigDecimal total = new BigDecimal(threadSet.size());
            BigDecimal contentPointTotal = new BigDecimal(0);
            BigDecimal interactivePointTotal = new BigDecimal(0);
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setGroupingUsed(false);
            for (String s : threadSet) {
                Map <String, String> threadMap = RecommendThreadDataService.threadHGetAll(s);
                if (threadMap == null || threadMap.isEmpty()) {
                    continue;
                }
                if (threadMap.containsKey("contentPoint")) {
                    contentPointTotal = contentPointTotal.add(new BigDecimal(threadMap.get("contentPoint")));
                }
                if (threadMap.containsKey("interactivePoint")) {
                    interactivePointTotal = interactivePointTotal.add(new BigDecimal(threadMap.get("interactivePoint")));
                }
            }
            BigDecimal contentPointAvg = contentPointTotal.compareTo(new BigDecimal(0)) > 0 && total.compareTo(new BigDecimal(0)) > 0 ? contentPointTotal.divide(total, 5, BigDecimal.ROUND_HALF_UP) : new BigDecimal(0);
            BigDecimal interactiveAvg = interactivePointTotal.compareTo(new BigDecimal(0)) > 0 && total.compareTo(new BigDecimal(0)) > 0 ? interactivePointTotal.divide(total, 5, BigDecimal.ROUND_HALF_UP) : new BigDecimal(0);

            BigDecimal authorInfluence = contentPointAvg.multiply(new BigDecimal("0.3")).add(interactiveAvg.multiply(new BigDecimal("0.4")))
                    .add(publishedPoint.multiply(new BigDecimal("0.2"))).add(browsedPoint.multiply(new BigDecimal("0.1")));
            authorInfluence = authorInfluence.multiply(new BigDecimal(1000));



            if (authorInfluence.compareTo(new BigDecimal(0)) > 0) {
                RedisActionEntity authorInfluencePointAction = new RedisActionEntity(RedisActionEnums.HSet.value(), "recommendData:authorId:" + authorId, "authorInfluencePoint", authorInfluence.toPlainString());
                collector.collect(authorInfluencePointAction);
            }
        }
    }


    private static void addToRedis(DataStream <RedisActionEntity> dataStream) {
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("10.19.52.120")
                .setMaxTotal(5000)
                .setMaxIdle(10)
                .setMinIdle(5)
                .setPort(6379)
                .setPassword("cece_xxwolo")
                .build();
        dataStream.addSink(new MyRedisSink <>(config , new MyRedisMapper<>()));
    }

}
