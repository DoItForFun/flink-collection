package com.flyai.recommend.entity;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author lizhe
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ThreadVectorEntity {
    private String id;
    private String content_vector;
    private String like_vector;
    private String comment_vector;
    private String create_time_vector;
    private String word_count_vector;
    private String image_score_vector;
    private String is_selfie_vector;
    private String topic_vector;

}
