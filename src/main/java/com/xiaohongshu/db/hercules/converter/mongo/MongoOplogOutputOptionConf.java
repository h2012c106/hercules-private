package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

public class MongoOplogOutputOptionConf extends BaseOptionsConf {

    public final static String NS = "oplog-namespace";
    public final static String OP = "upsert";

    public static final String OBJECT_ID_COL = "object-id";
    public static final String DEFAULT_OBJECT_ID_COL = "_id";

    private static final String OBJECT_ID_DELIMITER = ",";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(NS)
                .needArg(true)
                .necessary(true)
                .description("Namespace, the format is database.collection.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(OBJECT_ID_COL)
                .needArg(true)
                .list(true)
                .listDelimiter(OBJECT_ID_DELIMITER)
                .defaultStringValue(DEFAULT_OBJECT_ID_COL)
                .description("The column name for object id.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
