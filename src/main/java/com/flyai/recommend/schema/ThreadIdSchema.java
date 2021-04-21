package com.flyai.recommend.schema;

import com.alibaba.fastjson.JSON;
import com.flyai.recommend.entity.ThreadActionEntity;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;


/**
 * @author lizhe
 */
public class ThreadIdSchema implements KafkaSerializationSchema <String> {

    private final String topic;

    public ThreadIdSchema(String topic){
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord <byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(topic, s.getBytes(StandardCharsets.UTF_8));
    }
}
