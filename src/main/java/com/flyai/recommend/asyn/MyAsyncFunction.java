package com.flyai.recommend.asyn;

import com.flyai.recommend.classification.DataClassification;
import com.flyai.recommend.entity.BaseInfoEntity;
import com.flyai.recommend.entity.ThreadActionEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Supplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author lizhe
 */
public class MyAsyncFunction extends RichAsyncFunction<BaseInfoEntity , List <ThreadActionEntity>> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(final BaseInfoEntity baseInfoEntity, final ResultFuture<List <ThreadActionEntity>> resultFuture) throws InterruptedException {
        List <ThreadActionEntity> resultList = DataClassification.classificationData(baseInfoEntity);
        query(resultList, resultFuture);
    }

    private void query(final List<ThreadActionEntity> resultList, final ResultFuture<List <ThreadActionEntity>> resultFuture) {
        resultFuture.complete(Collections.singletonList(resultList));
    }
}
