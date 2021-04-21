package com.flyai.recommend.partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author lizhe
 */
public class ThreadIdPartitioner extends FlinkKafkaPartitioner {
    private static final long serialVersionUID = -6574482264347381518L;

    @Override
    public int partition(Object o, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return Math.abs(new String(bytes).hashCode() % ints.length);
    }
}
