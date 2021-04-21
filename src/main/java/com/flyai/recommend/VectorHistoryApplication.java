package com.flyai.recommend;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.ThreadEntity;
import com.flyai.recommend.entity.ThreadVectorActionEntity;
import com.flyai.recommend.entity.ThreadVectorEntity;
import com.flyai.recommend.mapper.MyMongodbSink;
import com.flyai.recommend.utils.*;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class VectorHistoryApplication {


    public static void main(String[] args) throws Exception {
        MongoCollection <Document> collection = MongodbUtils.getCollection("thread", "cece_thread");
        BasicDBObject condition = new BasicDBObject();
        condition.put("createTime" , new BasicDBObject("$gt" , Long.valueOf("1583829121000")));
        List <?> byCondition = MongodbUtils.getByCondition(collection, condition, ThreadEntity.class);
        List<ThreadVectorEntity> list = new ArrayList<>();
        for (Object o : byCondition) {
            ThreadEntity entity = (ThreadEntity) o;
            ThreadVectorEntity threadVectorEntity = new ThreadVectorEntity();
            threadVectorEntity.setId(entity.getThreadId());
            Map <String, String> map = new HashMap <>();
            map.put("texts" , entity.getContent());
            List<String> vecFromApi = VectorUtils.getVecFromApi(map);
            threadVectorEntity.setContent_vector(JSON.toJSONString(vecFromApi));
            int length = entity.getContent().length();
            // @todo 字数分组
            String wordCountVector = VectorUtils.wordCount(length);
            if(wordCountVector != null){
                threadVectorEntity.setWord_count_vector(wordCountVector);
            }
            // @todo 星期分组
            String weekVector = VectorUtils.week(entity.getCreateTime().toString());
            if(weekVector != null){
                threadVectorEntity.setCreate_time_vector(weekVector);
            }
            // @todo 点赞分组
            String likeVector = VectorUtils.likeOrComment(entity.getLikeCount());
            if(likeVector != null){
                threadVectorEntity.setLike_vector(likeVector);
            }
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
            list.add(threadVectorEntity);
            if(list.size() == 50){
                DruidUtils.insertBatch(ThreadVectorEntity.class , list);
                list.clear();
            }
        }
        if(list.size() > 0){
            DruidUtils.insertBatch(ThreadVectorEntity.class , list);
        }

    }
}
