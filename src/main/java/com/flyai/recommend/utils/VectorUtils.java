package com.flyai.recommend.utils;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

/**
 * @author lizhe
 */
@Slf4j
public class VectorUtils {
    public static List <String> getVecFromApi(Map <String , String> map) throws IOException {
        String url = "http://ai.flyai.com/ai";
        Map<String, String> params = new HashMap <>();
        params.put("project_id" , "cece_recommend_service_01");
        params.put("online" , "1");
        params.put("ab" , "2");
        params.put("prefix" , "cece");
        params.put("user_id" , map.get("userId"));
        params.put("texts" , map.get("texts"));
        String response =  HttpClient.doPost(url , params);
        List<String> result = new ArrayList<>();
        if(response != null){
            try{
                Map responseMap = JSON.parseObject(response, Map.class);
                if(responseMap != null
                        && responseMap.containsKey("status")
                        && responseMap.containsKey("output")
                        && Integer.parseInt(responseMap.get("status").toString()) == 200)
                {
                    Map output = JSON.parseObject(JSON.toJSONString(responseMap.get("output")), Map.class);
                    List<String> list = JSON.parseObject(output.get("vec").toString(), List.class);
                    if(list != null && !list.isEmpty()){
                        List<Object> temp = JSON.parseObject(JSON.toJSONString(list.get(0)) , List.class);
                        for (Object o : temp) {
                            result.add(o.toString());
                        }
                        return result;
                    }
                }
            }catch (Exception e){
                log.error(e.getMessage());
            }
        }
        return null;
    }

    public static String week(String timestamp){
        if(timestamp == null){
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        long time = Long.parseLong(timestamp);
        calendar.setTime(new Date(time));
        int pos = -1;
        switch (calendar.get(Calendar.DAY_OF_WEEK)){
            case Calendar.MONDAY:
                pos = 0;
                break;
            case Calendar.TUESDAY:
                pos = 1;
                break;
            case Calendar.WEDNESDAY:
                pos = 2;
                break;
            case Calendar.THURSDAY:
                pos = 3;
                break;
            case Calendar.FRIDAY:
                pos = 4;
                break;
            case Calendar.SATURDAY:
                pos = 5;
                break;
            case Calendar.SUNDAY:
                pos = 6;
                break;
            default:
                break;
        }
        if(pos >= 0){
            return OneHotUtils.getOneHotCode(7 , pos);
        }
        return null;
    }

    public static String wordCount(Integer length){
        if(length == null){
            return null;
        }
        if(length <= 5){
            return OneHotUtils.getOneHotCode(5 , 0);
        }
        if(length <= 10){
            return OneHotUtils.getOneHotCode(5 , 1);
        }
        if(length <= 30){
            return OneHotUtils.getOneHotCode(5 , 2);
        }
        if(length <= 100){
            return OneHotUtils.getOneHotCode(5 , 3);
        }
        return OneHotUtils.getOneHotCode(5 , 4);
    }

    public static String likeOrComment(Integer likeCount){
        if(likeCount == null){
            return null;
        }
        if(likeCount <= 10){
            return OneHotUtils.getOneHotCode(4 , 0);
        }
        if(likeCount <= 50){
            return OneHotUtils.getOneHotCode(4 , 1);
        }
        if(likeCount <= 100){
            return OneHotUtils.getOneHotCode(4 , 2);
        }
        return OneHotUtils.getOneHotCode(4 , 3);
    }

    public static String isSelfie(Integer isSelfie){
        if(isSelfie == null){
            return null;
        }
        if(isSelfie == 0){
            return OneHotUtils.getOneHotCode(2 , 0);
        }

        if(isSelfie == 1){
            return OneHotUtils.getOneHotCode(2 , 1);
        }
        return null;
    }

    public static String score(Double score){
        if(score == null){
            return null;
        }
        if(score <= 5.0){
            return OneHotUtils.getOneHotCode(2 , 0);
        }
        if(score > 5.0 && score <= 10){
            return OneHotUtils.getOneHotCode(2 , 1);
        }
        return null;
    }
}
