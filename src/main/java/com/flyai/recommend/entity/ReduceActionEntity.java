package com.flyai.recommend.entity;

import java.io.Serializable;

/**
 * @author lizhe
 */
public class ReduceActionEntity implements Serializable {
    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public Integer getBrowse() {
        return Browse;
    }

    public void setBrowse(Integer browse) {
        Browse = browse;
    }

    public Integer getDetail() {
        return Detail;
    }

    public void setDetail(Integer detail) {
        Detail = detail;
    }

    public Integer getPublishSave() {
        return PublishSave;
    }

    public void setPublishSave(Integer publishSave) {
        PublishSave = publishSave;
    }

    public Integer getPublish() {
        return Publish;
    }

    public void setPublish(Integer publish) {
        Publish = publish;
    }

    public Integer getShare() {
        return Share;
    }

    public void setShare(Integer share) {
        Share = share;
    }

    public Integer getLike() {
        return Like;
    }

    public void setLike(Integer like) {
        Like = like;
    }

    private String threadId;
    private Integer Browse;

    public Integer getBrowsed() {
        return Browsed;
    }

    public void setBrowsed(Integer browsed) {
        Browsed = browsed;
    }

    private Integer Browsed;
    private Integer Detail;
    private Integer PublishSave;
    private Integer Publish;

    public Integer getComment() {
        return Comment;
    }

    public void setComment(Integer comment) {
        Comment = comment;
    }

    private Integer Comment;
    private Integer Share;
    private Integer Like;

    @Override
    public String toString() {
        return "ReduceActionEntity{" +
                "threadId='" + threadId + '\'' +
                ", Browse=" + Browse +
                ", Browsed=" + Browsed +
                ", Detail=" + Detail +
                ", PublishSave=" + PublishSave +
                ", Publish=" + Publish +
                ", Comment=" + Comment +
                ", Share=" + Share +
                ", Like=" + Like +
                '}';
    }
}
