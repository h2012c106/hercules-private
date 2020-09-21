package com.xiaohongshu.db.hercules.redis.option;


import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class RedisOptionConf extends BaseOptionsConf {

    public final static String REDIS_HOST = "redis_host";
    public final static String REDIS_PORT = "redis_port";
    public static final String REDIS_PIPE_SIZE = "pipe_size";
    public  static final String REDIS_EXPIRE = "expire" ;
    public static final long DEFAULT_PIPE_SIZE = 5000;

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
                .name(REDIS_HOST)
                .needArg(true)
                .necessary(true)
                .description("redis host or ip for target.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(REDIS_PORT)
                .needArg(true)
                .necessary(true)
                .description("redis port for target.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(REDIS_PIPE_SIZE)
                .needArg(true)
                .description("redis pipeline size submits.")
                .defaultStringValue(String.valueOf(DEFAULT_PIPE_SIZE))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(REDIS_EXPIRE)
                .needArg(true)
                .description("redis expire time.")
                .build());

        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}