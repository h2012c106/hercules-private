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
    public static final String IGNORE_SPLIT_KEY_CHECK = "ignore-split-key-check";
    public static final String OBJECT_ID_COL = "object-id";
    public static final String DEFAULT_OBJECT_ID_COL = "_id";

    public static final String PASS_OBJECT_ID = "pass-object-id";
    private static final String OBJECT_ID_DELIMITER = ",";

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
        tmpList.add(SingleOptionConf.builder()
                .name(IGNORE_SPLIT_KEY_CHECK)
                .needArg(false)
                .description("If specified, will not abandon the situation that specifying a non-key column as split key.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(OBJECT_ID_COL)
                .needArg(true)
                .list(true)
                .listDelimiter(OBJECT_ID_DELIMITER)
                .defaultStringValue(DEFAULT_OBJECT_ID_COL)
                .description("The column name for object id.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PASS_OBJECT_ID)
                .description("If specified, the object id will be transferred.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
