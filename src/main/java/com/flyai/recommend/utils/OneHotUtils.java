package com.flyai.recommend.utils;

/**
 * @author lizhe
 */
public class OneHotUtils {
    public static String getOneHotCode(int length , int index){
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if(i == index){
                stringBuilder.append("1");
            }else{
                stringBuilder.append("0");
            }
        }
        return stringBuilder.toString();
    }
}
