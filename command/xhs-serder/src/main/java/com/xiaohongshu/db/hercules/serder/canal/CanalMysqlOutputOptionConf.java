package com.xiaohongshu.db.hercules.serder.canal;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class CanalMysqlOutputOptionConf extends BaseOptionsConf {

    public final static String SCHEMA_NAME = "canal-schema-name";
    public final static String TABLE_NAME = "canal-table-name";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new SerOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(SCHEMA_NAME)
                .needArg(true)
                .necessary(true)
                .description("The schema name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(TABLE_NAME)
                .needArg(true)
                .necessary(true)
                .description("The table name.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
