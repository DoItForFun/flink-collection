package com.flyai.recommend.enums;


import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.base.Objects;

/**
 * @author lizhe
 */
public enum EventEnums {
    Add(0 , "Insert"),
    Browse(3 , "Browse"),
    Detail(4 , "Detail"),
    Like(5 , "Like"),
    Comment(6 , "Comment"),
    AuthorReply(10 , "AuthorReply"),
    Share(11 , "Share"),
    Collect(12 , "Collect"),
    ImageScore(13 , "ImageScore");

    private final Integer value;
    private final String desc;

    EventEnums(Integer value , String desc){
        this.value = value;
        this.desc = desc;
    }

    public static EventEnums from(Integer value) {
        for (EventEnums status : EventEnums.values()) {
            if (Objects.equal(status.value(), value)) {
                return status;
            }
        }
        return null;
    }

    public Integer value() {
        return this.value;
    }
    public String desc(){
        return this.desc;
    }
}
