package com.xiaohongshu.db.hercules.rdbms.option;

import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.List;

public final class RDBMSOptionsConf extends BaseOptionsConf {

    public static final String CONNECTION = "connection";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER = "driver";

    public static final String TABLE = "table";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(CONNECTION)
                .needArg(true)
                .necessary(true)
                .description("The jdbc connection url.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(USERNAME)
                .needArg(true)
                .description("The database username.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PASSWORD)
                .needArg(true)
                .description("The database password.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DRIVER)
                .needArg(true)
                .necessary(true)
                .description("The jdbc driver class name, e.g. 'com.mysql.jdbc.Driver'.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
