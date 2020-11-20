package com.xiaohongshu.db.hercules.serder.redis;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerOptionsConf;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jamesqq on 2020/11/3.
 */
public class RedisStringKVOutputOptionConf extends BaseOptionsConf {

    public final static String FORMAT = "string-format";

    public enum StringKVFormat {
        JSONTOSTRING,
        JSONTOMAP;

        public static StringKVFormat valueOfIgnoreCase(String value) {
            for (StringKVFormat format : StringKVFormat.values()) {
                if (StringUtils.equalsIgnoreCase(format.name(), value)) {
                    return format;
                }
            }
            throw new ParseException("Illegal format type: " + value);
        }
    }

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new SerOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(FORMAT)
                .needArg(true)
                .description("string format.")
                .necessary(true)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
