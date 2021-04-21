package com.flyai.recommend.entity;

import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class ThreadVectorActionEntity {
    private String threadId;
    private Integer create;
    private Integer like;
    private Integer comment;
    private Integer imageScore;
    private Integer topic;
}
