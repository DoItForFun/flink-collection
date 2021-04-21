package com.flyai.recommend.mapper;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.ThreadVectorEntity;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;


/**
 * @author lizhe
 */
@Slf4j
public class MyMongodbSink<IN> extends RichSinkFunction <IN> {

    private  MongoClient mongoClient;
    private  String dbName;
    private  String collectionName;

    public MyMongodbSink(String db, String collection){
        this.dbName = db;
        this.collectionName = collection;
    }

    public void invoke(IN input){
        ThreadVectorEntity data = (ThreadVectorEntity) input;
        try{
            MongoCollection <Document> collection = mongoClient.getDatabase(this.dbName).getCollection(this.collectionName);
            Document document = JSON.parseObject(JSON.toJSONString(data), Document.class);
            Bson condition = Filters.eq("_id", ((ThreadVectorEntity) input).getId());
            Document set = collection.findOneAndUpdate(condition , new Document("$set" , document));
            if(set == null){
                collection.insertOne(document);
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
