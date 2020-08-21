package com.xiaohongshu.db.hercules.mongodb.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseInputOptionsConf;
import com.xiaohongshu.db.hercules.mongodb.MongoDBUtils;

import java.util.ArrayList;
import java.util.List;

public final class MongoDBInputOptionsConf extends BaseOptionsConf {

    public static final String QUERY = "query";
    public static final String SPLIT_BY = "split-by";
    public static final String IGNORE_SPLIT_KEY_CHECK = "ignore-split-key-check";
    public static final String BATCH_SIZE = "batch-size";

    private static final int DEFAULT_BATCH_SIZE = 101;

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
                        "If not set, default to [%s].", MongoDBUtils.ID))
                .defaultStringValue(MongoDBUtils.ID)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(IGNORE_SPLIT_KEY_CHECK)
                .needArg(false)
                .description("If specified, will not abandon the situation that specifying a non-key column as split key.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BATCH_SIZE)
                .needArg(true)
                .description("The batch size documents fetched from server, default to: " + DEFAULT_BATCH_SIZE)
                .defaultStringValue(String.valueOf(DEFAULT_BATCH_SIZE))
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
