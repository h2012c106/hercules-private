package com.xiaohongshu.db.hercules.kafka.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class KafkaOptionConf extends BaseOptionsConf {

    public final static String BOOTSTRAP_SERVERS = "bootstrap-servers";
    public final static String RETRIES_CONFIG = "retries";
    public final static String BATCH_SIZE_CONFIG = "batch-size";
    public final static String LINGER_MS_CONFIG = "linger";

    public final static String LINGER_MS_DEFAULT = "5";
    public final static String BATCH_SIZE_DEFAULT = "50000";
    public final static String RETRIES_DEFAULT = "3";

    public final static String TOPIC = "kafka-topic";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new KVOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(BOOTSTRAP_SERVERS)
                .needArg(true)
                .necessary(true)
                .description("Bootstrap servers used to connect kafka.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(RETRIES_CONFIG)
                .needArg(true)
                .defaultStringValue(RETRIES_DEFAULT)
                .description("Retries for kafka producer. Default: " + RETRIES_DEFAULT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BATCH_SIZE_CONFIG)
                .needArg(true)
                .defaultStringValue(BATCH_SIZE_DEFAULT)
                .description("Kafka producer batch size. Default: " + BATCH_SIZE_DEFAULT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(LINGER_MS_CONFIG)
                .needArg(true)
                .defaultStringValue(LINGER_MS_DEFAULT)
                .description("Kafka producer linger time. Default: " + LINGER_MS_DEFAULT)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(TOPIC)
                .needArg(true)
                .necessary(true)
                .description("Kafka topic to send message.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}