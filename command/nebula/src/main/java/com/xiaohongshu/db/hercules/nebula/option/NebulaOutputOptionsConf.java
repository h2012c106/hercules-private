package com.xiaohongshu.db.hercules.nebula.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseInputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class NebulaOutputOptionsConf extends BaseOptionsConf {

    public static final String BATCH_SIZE = "batch-size";

    private static final int DEFAULT_BATCH_SIZE = 256;

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseInputOptionsConf(),
                new NebulaOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(BATCH_SIZE)
                .needArg(true)
                .description("The write batch size, default to: " + DEFAULT_BATCH_SIZE)
                .defaultStringValue(String.valueOf(DEFAULT_BATCH_SIZE))
                .validateFunction(SingleOptionConf.NUMBER_AND_GT_ZERO)
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {
    }
}
