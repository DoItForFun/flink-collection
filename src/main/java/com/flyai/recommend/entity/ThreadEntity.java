package com.flyai.recommend.entity;

import lombok.Data;

/**
 * @author lizhe
 */
@Data
public class ThreadEntity {
    private String threadId;
    private String content;
    private String gid;
    private String imageUrl;
    private String voiceUrl;
    private String videoUrl;
    private String otherUrl;
    private String otherUrlInfo;
    private String authorId;
    private String title;
    private Integer replyCount;
    private Integer likeCount;
    private Integer deleted;
    private Integer closed;
    private Long createTime;
    private Long updateTime;
    private Integer contentChecked;
    private Integer imageChecked;
    private Integer open;
    private String imageType;
    private String type;
    private Integer prosecuted;
    private Integer voiceDuration;
    private Integer adult;
    private Integer voiceTime;
    private Integer videoTime;
    private Integer isSelfie;
    private Double score;
    private Integer wordCount;
}
