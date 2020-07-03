package com.xiaohongshu.db.hercules.converter;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;

import java.util.ArrayList;
import java.util.List;

public class CanalOutputOptionConf extends BaseOptionsConf {

    public final static String SCHEMA_NAME = "canal-schemaName";
    public final static String TABLE_NAME = "canal-tableName";
    public final static String KEY = "canal-key";

    public final static String COLLECTION = "canal-collection";
    public final static String KV_CONVERTER = "kv-converter";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new KafkaOptionConf()
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
        switch (options.getString(KV_CONVERTER,"")){
            case "MysqlConverter":

        }
    }
}
