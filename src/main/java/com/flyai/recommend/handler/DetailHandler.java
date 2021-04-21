package com.flyai.recommend.handler;


import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.enums.AuthorEnums;
import com.flyai.recommend.enums.EventEnums;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


/**
 * @author lizhe
 */
@Slf4j
public class DetailHandler {
    public static List <ThreadActionEntity> process(BaseInfoEntity baseInfoEntity){
        List <ThreadActionEntity> list = new ArrayList <>();
        String threadId = baseInfoEntity.getItemId().toString();
        ThreadActionEntity threadActionEntity = new ThreadActionEntity();
        threadActionEntity.setCount(1);
        threadActionEntity.setAction(EventEnums.Detail.desc());
        threadActionEntity.setTime(baseInfoEntity.getActionTime());
        threadActionEntity.setThreadId(threadId);
        list.add(threadActionEntity);

        ThreadActionEntity threadActionEntity2 = new ThreadActionEntity();
        threadActionEntity2.setCount(1);
        threadActionEntity2.setTime(baseInfoEntity.getActionTime());
        threadActionEntity2.setThreadId(threadId);
        threadActionEntity2.setAction(AuthorEnums.Browse.desc());
        list.add(threadActionEntity2);

        return list;
    }
}
