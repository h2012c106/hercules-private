package com.xiaohongshu.db.hercules.clickhouse.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.FETCH_SIZE;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.RANDOM_FUNC_NAME;

public final class ClickhouseInputOptionsConf extends BaseOptionsConf {

    public static final String DEFAULT_RANDOM_FUNC_NAME = "(RAND() / 4294967295)";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new RDBMSInputOptionsConf(),
                new ClickhouseOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(RANDOM_FUNC_NAME)
                .needArg(true)
                .description("The random function used at balance mode sampling.")
                .defaultStringValue(DEFAULT_RANDOM_FUNC_NAME)
                .build());
        // clickhouse jdbc不支持fetch size
        clearOption(tmpList, FETCH_SIZE);
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
