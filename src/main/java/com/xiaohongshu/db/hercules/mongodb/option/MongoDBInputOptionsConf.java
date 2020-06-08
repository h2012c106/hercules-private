package com.xiaohongshu.db.hercules.mongodb.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseInputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;

import java.util.ArrayList;
import java.util.List;

public final class MongoDBInputOptionsConf extends BaseOptionsConf {

    public static final String QUERY = "query";
    public static final String SPLIT_BY = "split-by";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseInputOptionsConf(),
                new MongoDBOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(QUERY)
                .needArg(true)
                .description("The query json.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SPLIT_BY)
                .needArg(true)
                .description(String.format("The column that splitting map will depend on. " +
                        "If not set, default to [%s].", MongoDBManager.ID))
                .defaultStringValue(MongoDBManager.ID)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
