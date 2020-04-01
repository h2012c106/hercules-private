package com.xiaohongshu.db.hercules.rdbms.common.options;

import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;

import java.util.List;

public class RDBMSOptionsConf extends BaseDataSourceOptionsConf {

    public static final String CONNECTION = "connection";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER = "driver";

    public static final String TABLE = "table";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
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
}
