package com.xiaohongshu.db.hercules.mongodb.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;

import java.util.ArrayList;
import java.util.List;

public final class MongoDBOptionsConf extends BaseOptionsConf {

    public static final String CONNECTION = "connection";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "password";
    public static final String AUTHDB = "authdb";
    public static final String DATABASE = "database";
    public static final String COLLECTION = "collection";

    private static final String CONNECTION_DELIMITER = ",";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new TableOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(CONNECTION)
                .needArg(true)
                .necessary(true)
                .description(String.format("The mongodb connection url, format: <host>:<port>. " +
                        "Allow cluster, connections separated by '%s'.", CONNECTION_DELIMITER))
                .list(true)
                .listDelimiter(CONNECTION_DELIMITER)
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
                .name(AUTHDB)
                .needArg(true)
                .description("The database auth database name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DATABASE)
                .needArg(true)
                .necessary(true)
                .description("The collection source database name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLLECTION)
                .needArg(true)
                .necessary(true)
                .description("The collection name.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
