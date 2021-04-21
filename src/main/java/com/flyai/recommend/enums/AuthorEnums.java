package com.flyai.recommend.enums;

/**
 * @author lizhe
 */
public enum AuthorEnums {
    Publish(1 , "Publish"),
    Browse(2 , "Browsed");

    private final Integer value;
    private final String desc;

    AuthorEnums(Integer value , String desc){
        this.value = value;
        this.desc = desc;
    }

    public String desc() {
        return this.desc;
    }
    public Integer value() {
        return this.value;
    }
}
