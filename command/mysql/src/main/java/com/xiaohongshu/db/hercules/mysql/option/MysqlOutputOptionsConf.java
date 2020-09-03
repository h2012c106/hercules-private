package com.xiaohongshu.db.hercules.mysql.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public final class MysqlOutputOptionsConf extends BaseOptionsConf {

    public static final String ALLOW_ZERO_DATE = "allow-zero-date";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new RDBMSOutputOptionsConf(),
                new MysqlOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_ZERO_DATE)
                .needArg(false)
                .description("Whether to allow inserting '0000-00-00 00:00:00' as timestamp.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
