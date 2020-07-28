package com.xiaohongshu.db.hercules.converter.mysql;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

public class CanalMysqlOutputOptionConf extends BaseOptionsConf {

    public final static String SCHEMA_NAME = "canal-schema-name";
    public final static String TABLE_NAME = "canal-table-name";
    public final static String KEY = "canal-key";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
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
        tmpList.add(SingleOptionConf.builder()
                .name(KEY)
                .needArg(true)
                .necessary(true)
                .description("The key column to add to canal entry.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
