package com.flyai.recommend.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.enums.EventEnums;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author lizhe
 */
@Slf4j
public class BrowseHandler{
    public static List<ThreadActionEntity> process(BaseInfoEntity baseInfoEntity){
        List<ThreadActionEntity> list = new ArrayList<>();
        List<String> itemList = new ArrayList<>();
        String properties = baseInfoEntity.getItemId().toString();
        try{
            itemList = JSON.parseObject(properties, List.class);
        }catch (Exception e){
//            log.info("transform to list failed:{}" , e.getMessage());
        }
        if(itemList.isEmpty()){
            try{
                Map<String, String> map = JSONObject.parseObject(baseInfoEntity.getItemId().toString(), Map.class);
                for(Map.Entry<String, String> entry : map.entrySet()){
                    itemList.add(entry.getKey());
                }
            }catch (Exception e){
//                log.error("error data:{}" , e.getMessage());
                return null;
            }
        }
        for (String threadId : itemList) {
            if(threadId == null || threadId.isEmpty()){
                continue;
            }
            ThreadActionEntity threadActionEntity = new ThreadActionEntity();
            threadActionEntity.setAction(EventEnums.Browse.desc());
            threadActionEntity.setThreadId(threadId);
            threadActionEntity.setCount(1);
            threadActionEntity.setTime(baseInfoEntity.getActionTime());
            list.add(threadActionEntity);
        }
        return list;
    }
}
