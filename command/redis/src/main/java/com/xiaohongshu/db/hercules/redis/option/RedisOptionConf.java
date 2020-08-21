package com.xiaohongshu.db.hercules.redis.option;


import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf;

import java.util.ArrayList;
import java.util.List;

public class RedisOptionConf extends BaseOptionsConf {

    public final static String REDIS_HOST = "redis_host";
    public final static String REDIS_PORT = "redis_port";

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

        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}