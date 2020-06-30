package com.xiaohongshu.db.hercules.mysql.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public final class MysqlInputOptionsConf extends BaseOptionsConf {

    public static final String DEFAULT_RANDOM_FUNC_NAME = "RAND()";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new RDBMSInputOptionsConf(),
                new MysqlOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(RDBMSInputOptionsConf.RANDOM_FUNC_NAME)
                .needArg(true)
                .description("The random function used at balance mode sampling.")
                .defaultStringValue(DEFAULT_RANDOM_FUNC_NAME)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }

}
