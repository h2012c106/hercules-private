package com.xiaohongshu.db.hercules.bson.option;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class BsonOptionsConf extends BaseOptionsConf {

    public static final String DIR = "dir";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(DIR)
                .needArg(true)
                .necessary(true)
                .description("The bson files' directory.")
                .build());
        return res;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
