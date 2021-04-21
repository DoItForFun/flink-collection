package com.flyai.recommend.handler;

import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.enums.AuthorEnums;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lizhe
 */
@Slf4j
public class AddHandler {
    public static List<ThreadActionEntity> process(BaseInfoEntity baseInfoEntity){
        List<ThreadActionEntity> list = new ArrayList <>();
        ThreadActionEntity threadActionEntity = new ThreadActionEntity();
        threadActionEntity.setCount(1);
        threadActionEntity.setTime(baseInfoEntity.getActionTime());
        threadActionEntity.setThreadId(baseInfoEntity.getItemId().toString());
        threadActionEntity.setAction(AuthorEnums.Publish.desc());
        list.add(threadActionEntity);

        ThreadActionEntity threadActionEntity2 = new ThreadActionEntity();
        threadActionEntity2.setCount(1);
        threadActionEntity2.setTime(baseInfoEntity.getActionTime());
        threadActionEntity2.setThreadId(baseInfoEntity.getItemId().toString());
        threadActionEntity2.setAction("PublishSave");
        list.add(threadActionEntity2);
        return list;
    }
}
