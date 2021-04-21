package com.flyai.recommend.utils;

import com.flyai.recommend.schema.ThreadIdSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

/**
 * @author lizhe
 */
public class KafkaPropertiesUtils {
    private final Properties kafkaProperties = new Properties();
    private String topic;

    public KafkaPropertiesUtils(){}

    public KafkaPropertiesUtils(String serversKey , String groupKey , String topicKey) throws Exception{
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/application.properties"));
        this.kafkaProperties.setProperty("bootstrap.servers", properties.getProperty(serversKey));
        this.kafkaProperties.setProperty("group.id", properties.getProperty(groupKey));
        this.topic = properties.getProperty(topicKey);
    }

    public FlinkKafkaConsumer <String> getKafkaConsumer(){
        List<String> topics = Arrays.asList(this.topic.split(","));
        return new FlinkKafkaConsumer <String>(topics, new SimpleStringSchema(), this.kafkaProperties);
    }

    public FlinkKafkaProducer<String> getKafkaProducer(){
        this.kafkaProperties.remove("group.id");
        this.kafkaProperties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1000 * 60 * 3 + "");
        this.kafkaProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        this.kafkaProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new FlinkKafkaProducer<String>("recommend_to_be_processed" , new ThreadIdSchema("recommend_to_be_processed") , this.kafkaProperties , FlinkKafkaProducer.Semantic.EXACTLY_ONCE , FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
    }
}
