package com.xiaohongshu.db.hercules.redis.option;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.*;

public class RedisOptionConf extends BaseOptionsConf {

    public final static String REDIS_HOST = "redis_host";
    public final static String REDIS_PORT = "redis_port";
    public static final String REDIS_PIPE_SIZE = "pipe_size";
    public static final long DEFAULT_PIPE_SIZE = 5000;
    public static final String REDIS_WRITE_TPYE = "write_type";
    public static final String REDIS_SCORE_NAME = "score-name";

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
                .name(REDIS_WRITE_TPYE)
                .needArg(true)
                .description("redis target write type.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(REDIS_SCORE_NAME)
                .needArg(true)
                .description("redis score name.")
                .build());

        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }

    @Override
    protected void innerProcessOptions(GenericOptions options) {
        // 面儿上看起来是kv的形式，但是在schema negotiate的时候还得老老实实按照table的形式来，不然乱套了，所以还是把信息拼成一张table
        String keyName = options.getString(KVOptionsConf.KEY_NAME, null);
        String valueName = options.getString(KVOptionsConf.VALUE_NAME, null);
        String scoreName = options.getString(REDIS_SCORE_NAME, null);

        if(scoreName == null)
            options.set(COLUMN, new String[]{keyName, valueName});
        else
            options.set(COLUMN, new String[]{keyName, valueName, scoreName});

        JSONObject json = new JSONObject();
        if (options.hasProperty(KVOptionsConf.KEY_TYPE)) {
            json.put(keyName, options.getString(KVOptionsConf.KEY_TYPE, null));
        }
        if (options.hasProperty(KVOptionsConf.VALUE_TYPE)) {
            json.put(valueName, options.getString(KVOptionsConf.VALUE_TYPE, null));
        }

        options.set(COLUMN_TYPE, json.toJSONString());

        options.set(INDEX, keyName);
        options.set(UNIQUE_KEY, keyName);
    }
}