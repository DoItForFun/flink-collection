package com.flyai.recommend.utils;

import com.alibaba.fastjson.JSON;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lizhe
 */
public class MongodbUtils {

    private static MongoClient mongoClient;

    static {
        mongoClient = new MongoClient(new MongoClientURI("mongodb://root:xxwolo_root@10.19.49.101:27017,10.19.84.234:27017,10.19.136.252:27017/?authSource=admin&slaveOk=true&replicaSet=rs1&write=1&readPreference=secondaryPreferred&connectTimeoutMS=30000"));
    }

    public MongodbUtils(String host, String author, String password) {
        mongoClient = new MongoClient(new MongoClientURI("mongodb://" + author + ":" + password + "@" + host + "/?authSource=admin&slaveOk=true&replicaSet=rs1&write=1&readPreference=secondaryPreferred&connectTimeoutMS=300000"));
    }

    public static MongoDatabase getDatabase(String database) {
        return mongoClient.getDatabase(database);
    }

    public static MongoCollection <Document> getCollection(String database, String collectionName) {
        MongoCollection <Document> collection = mongoClient.getDatabase(database).getCollection(collectionName);
        return collection;
    }

    public static BasicDBObject getCondition(String field , Object value){
        return new BasicDBObject(field ,  value);
    }

    public static List<Object> getByCondition(MongoCollection <Document> mongoCollection , BasicDBObject condition , BasicDBObject sort , Class<?> clazz){
        List<Object> list = new ArrayList <>();
        FindIterable <Document> documents = mongoCollection.find(condition).sort(sort);
        MongoCursor<Document> mongoCursor = documents.iterator();
        while (mongoCursor.hasNext()){
            list.add(JSON.parseObject(JSON.toJSONString(mongoCursor.next()) , clazz));
        }
        return list;
    }

    public static List<?> getByCondition(MongoCollection <Document> mongoCollection , BasicDBObject condition , Class<?> clazz){
        List<Object> list = new ArrayList <>();
        FindIterable <Document> documents = mongoCollection.find(condition);
        MongoCursor<Document> mongoCursor = documents.iterator();
        while (mongoCursor.hasNext()){
            list.add(JSON.parseObject(JSON.toJSONString(mongoCursor.next()) , clazz));
        }
        return list;
    }

    public static Object getOneByCondition(MongoCollection <Document> mongoCollection , BasicDBObject condition , Class<?> clazz){
        FindIterable <Document> documents = mongoCollection.find(condition);
        if(documents.first() == null){
            return null;
        }
        MongoCursor<Document> mongoCursor = documents.iterator();
        if(mongoCursor.hasNext()){
            return JSON.parseObject(JSON.toJSONString(mongoCursor.next()) , clazz);
        }
       return null;
    }

    public static void insertOne(MongoCollection <Document> mongoCollection , Object insert){
        Document document = JSON.parseObject(JSON.toJSONString(insert), Document.class);
        mongoCollection.insertOne(document);
    }

    public static void insertMany(MongoCollection <Document> mongoCollection , List<?> dataList){
        List<Document> insertList = new ArrayList<>();
        for (Object o : dataList) {
            Document document = JSON.parseObject(JSON.toJSONString(o), Document.class);
            insertList.add(document);
        }
        mongoCollection.insertMany(insertList);
    }

    public static void updateOneByPrimaryKey(MongoCollection <Document> mongoCollection , Object update){
        Map<String, Object> updateMap = JSON.parseObject(JSON.toJSONString(update) , Map.class);
        if(updateMap.get("_id") == null){
            return;
        }

        Document updatedDocument = new Document();
        for(Map.Entry<String, Object> entry : updateMap.entrySet()){
            if(entry == null
                    || entry.getKey() == null
                    || entry.getValue() == null
                    ||entry.getKey().equals("_id")
            ){
                continue;
            }
            updatedDocument.put(entry.getKey() , entry.getValue());
        }
        Document updated = new Document("$set" , updatedDocument);
        Bson condition = Filters.eq("_id", updateMap.get("_id"));
        mongoCollection.updateOne(condition, updated);
    }

    public static void close(){
        if(mongoClient != null){
            mongoClient.close();
        }
    }
}
