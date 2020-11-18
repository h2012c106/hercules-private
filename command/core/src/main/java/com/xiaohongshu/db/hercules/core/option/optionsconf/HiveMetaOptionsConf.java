package com.xiaohongshu.db.hercules.core.option.optionsconf;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.LinkedList;
import java.util.List;

public class HiveMetaOptionsConf extends BaseOptionsConf {

    public static final String HIVE_META_CONNECTION = "hive-meta-connection";
    public static final String HIVE_META_USER = "hive-meta-user";
    public static final String HIVE_META_PASSWORD = "hive-meta-password";
    public static final String HIVE_META_DRIVER = "hive-meta-driver";
    public static final String HIVE_DATABASE = "hive-database";
    public static final String HIVE_TABLE = "hive-table";

    public static final String DEFAULT_HIVE_META_DRIVER = "com.mysql.jdbc.Driver";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new LinkedList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_META_CONNECTION)
                .needArg(true)
                .description("The hive metadata jdbc connection url.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_META_USER)
                .needArg(true)
                .description("The hive metadata database username.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_META_PASSWORD)
                .needArg(true)
                .description("The hive metadata database password.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_DATABASE)
                .needArg(true)
                .description("The database of table needed to be fetched hive metadata.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_TABLE)
                .needArg(true)
                .description("The table needed to be fetched hive metadata.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_META_DRIVER)
                .needArg(true)
                .description("The hive metadata jdbc driver class name, default: " + DEFAULT_HIVE_META_DRIVER)
                .defaultStringValue(DEFAULT_HIVE_META_DRIVER)
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
