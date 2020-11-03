package com.xiaohongshu.db.hercules.bson.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class BsonOutputOptionsConf extends BaseOptionsConf {

    public static final String DELETE_TARGET_DIR = "delete-target-dir";
    public static final String COMPRESS_CODEC = "compress-codec";
    public static final String DEFAULT_COMPRESS_CODEC = "snappy";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BsonOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> res = new ArrayList<>();
        res.add(SingleOptionConf.builder()
                .name(DELETE_TARGET_DIR)
                .needArg(false)
                .description("If specified, will recursively delete target dir.")
                .build());
        res.add(SingleOptionConf.builder()
                .name(COMPRESS_CODEC)
                .needArg(true)
                .defaultStringValue(DEFAULT_COMPRESS_CODEC)
                .description("Compress codec used to compress. This will function after compress is set to true.")
                .build());
        return res;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
