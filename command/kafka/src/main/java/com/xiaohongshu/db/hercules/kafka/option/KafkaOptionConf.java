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
    public final static String MAX_REQUEST_SIZE_CONFIG = "max-request-size";
    public final static int DEFAULT_MAX_REQUEST_SIZE_CONFIG = 5000000;

    public final static String LINGER_MS_DEFAULT = "100";
    public final static String BATCH_SIZE_DEFAULT = "50000";
    public final static String RETRIES_DEFAULT = "3";

    public final static String TOPIC = "kafka-topic";
    public final static String DELETE_BEFORE_RUN = "delete-topic-records";
    public final static boolean DEFAULT_DELETE_BEFORE_RUN = false;

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
        tmpList.add(SingleOptionConf.builder()
                .name(MAX_REQUEST_SIZE_CONFIG)
                .needArg(true)
                .description("The maximum size of a request in bytes. The default is " + DEFAULT_MAX_REQUEST_SIZE_CONFIG + "bytes")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(DELETE_BEFORE_RUN)
                .needArg(false)
                .description("Whether delete topic records before run. The default is " + DEFAULT_DELETE_BEFORE_RUN)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
