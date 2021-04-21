package com.flyai.recommend.handler;

import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.enums.EventEnums;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


/**
 * @author lizhe
 */
@Slf4j
public class CommentHandler {
    public static List <ThreadActionEntity> process(BaseInfoEntity baseInfoEntity){
        List <ThreadActionEntity> list = new ArrayList <>();
        ThreadActionEntity threadActionEntity = new ThreadActionEntity();
        threadActionEntity.setAction(EventEnums.Comment.desc());
        threadActionEntity.setThreadId(baseInfoEntity.getItemId().toString());
        threadActionEntity.setCount(1);
        threadActionEntity.setTime(baseInfoEntity.getActionTime());
        list.add(threadActionEntity);
        return list;
    }
}
