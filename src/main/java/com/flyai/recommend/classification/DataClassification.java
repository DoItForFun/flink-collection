package com.flyai.recommend.classification;

import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import com.flyai.recommend.enums.EventEnums;
import com.flyai.recommend.handler.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.List;

/**
 * @author lizhe
 */
@Slf4j
public class DataClassification {
    public static List<ThreadActionEntity> classificationData(BaseInfoEntity baseInfoEntity) {
        List<ThreadActionEntity> threadActionEntity = null;
        EventEnums eventEnums = EventEnums.from(Integer.parseInt(baseInfoEntity.getActionId()));
        if(eventEnums == null){
            return null;
        }
        switch (eventEnums) {
            case Add:
                threadActionEntity = AddHandler.process(baseInfoEntity);
                break;
            case Browse:
                threadActionEntity = BrowseHandler.process(baseInfoEntity);
                break;
            case Detail:
                threadActionEntity = DetailHandler.process(baseInfoEntity);
                break;
            case Like:
                threadActionEntity = LikeHandler.process(baseInfoEntity);
                break;
            case Share:
                threadActionEntity = ShareHandler.process(baseInfoEntity);
                break;
            case Comment:
                threadActionEntity = CommentHandler.process(baseInfoEntity);
                break;
            default:
                log.debug("input data does not match the event : {}", baseInfoEntity);
                break;
        }
        return threadActionEntity;
    }
}
