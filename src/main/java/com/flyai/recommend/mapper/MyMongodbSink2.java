package com.flyai.recommend.mapper;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.ThreadEventEntity;
import com.flyai.recommend.entity.ThreadVectorEntity;
import com.flyai.recommend.entity.UserEntity;
import com.flyai.recommend.entity.UserVectorEntity;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;


/**
 * @author lizhe
 */
@Slf4j
public class MyMongodbSink2<IN> extends RichSinkFunction <IN> {

    private  MongoClient mongoClient;
    private  String dbName;
    private  String collectionName;

    public MyMongodbSink2(String db, String collection){
        this.dbName = db;
        this.collectionName = collection;
    }

    public void invoke(IN input){
        MongoCollection <Document> collection = mongoClient.getDatabase(this.dbName).getCollection(this.collectionName);
        Document document = null;
        String id = null;
        if(input instanceof UserEntity){
            UserEntity data = (UserEntity) input;
            document = JSON.parseObject(JSON.toJSONString(data), Document.class);
            id = data.getId();
        }
        if(input instanceof  UserVectorEntity){
            UserVectorEntity data = (UserVectorEntity) input;
            document = JSON.parseObject(JSON.toJSONString(data), Document.class);
            id = data.getId();
        }
        if(input instanceof ThreadEventEntity){
            ThreadEventEntity data = (ThreadEventEntity) input;
            document = JSON.parseObject(JSON.toJSONString(data) ,Document.class);
            id = data.getId();
        }
        try{
            if(document != null){
                Bson condition = Filters.eq("_id", id);
                Document set = collection.findOneAndUpdate(condition , new Document("$set" , document));
                if(set == null){
                    collection.insertOne(document);
                }
            }
        }catch (Exception e){
            log.error("sink failed:{}" , e.getMessage());
        }
    }

    public void open(Configuration parameters) throws Exception {
        this.mongoClient = new MongoClient(new MongoClientURI("mongodb://root:xxwolo_root@10.19.49.101:27017,10.19.84.234:27017,10.19.136.252:27017/?authSource=admin&slaveOk=true&replicaSet=rs1&write=1&readPreference=secondaryPreferred&connectTimeoutMS=300000"));
    }


    public void close() throws IOException {
        if(this.mongoClient != null){
           try{
               this.mongoClient.close();
           }catch (Exception e){
               log.error("mongoClient close failed:{}" , e.getMessage());
           }
        }
    }
}
