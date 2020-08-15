package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

public class MongoOplogOutputOptionConf extends BaseOptionsConf {

    public final static String NS = "oplog-namespace";
    public final static String OP = "upsert";

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
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
