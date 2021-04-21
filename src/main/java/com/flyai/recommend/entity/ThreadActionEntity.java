package com.flyai.recommend.entity;


import java.io.Serializable;

/**
 * @author lizhe
 */
public class ThreadActionEntity implements Serializable {

    private static final long serialVersionUID = 7308372481906746869L;

    private String threadId;
    private String action;
    private Integer count;

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    private Long time;

    @Override
    public String toString() {
        return "ThreadActionEntity{" +
                "threadId='" + threadId + '\'' +
                ", action='" + action + '\'' +
                ", count=" + count +
                ", time=" + time +
                '}';
    }


    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }


    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }


    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }


}
