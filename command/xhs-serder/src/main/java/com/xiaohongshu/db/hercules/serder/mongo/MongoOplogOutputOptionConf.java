package com.xiaohongshu.db.hercules.serder.mongo;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class MongoOplogOutputOptionConf extends BaseOptionsConf {

    public final static String NS = "oplog-namespace";
    public final static String FORMAT = "oplog-format";

    public final static String OP = "upsert";

    public enum OplogFormat{
        DEFAULT,
        JSON;

        public static OplogFormat valueOfIgnoreCase(String value) {
            for (OplogFormat format : OplogFormat.values()) {
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
                .name(NS)
                .needArg(true)
                .necessary(true)
                .description("Namespace, the format is database.collection.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(FORMAT)
                .needArg(true)
                .description("Oplog format, standard oplog(default_format) format or json format(json_format).")
                .necessary(true)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
