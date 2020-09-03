package com.xiaohongshu.db.hercules.hbase.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseOutputOptionsConf;

import java.util.ArrayList;
import java.util.List;

public final class HBaseOutputOptionsConf extends BaseOptionsConf {

    public final static String COLUMN_FAMILY = "column-family";

    public static final String MAX_WRITE_THREAD_NUM = "max-threads";
    public static final int DEFAULT_MAX_WRITE_THREAD_NUM = 5;

    public static final String WRITE_BUFFER_SIZE = "writebuffersize";
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final String CONVERT_COLUMN_NAME = "converted-column-name";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new HBaseOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN_FAMILY)
                .needArg(true)
                .necessary(true)
                .description("Column Family to Scan.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(WRITE_BUFFER_SIZE)
                .needArg(true)
                .description(String.format("The write buffer size, default %d bytes.", DEFAULT_WRITE_BUFFER_SIZE))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(MAX_WRITE_THREAD_NUM)
                .needArg(true)
                .description(String.format("The write maximum threads num, default %d threads.", DEFAULT_MAX_WRITE_THREAD_NUM))
                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(CONVERT_COLUMN_NAME)
//                .needArg(true)
//                .description("The qualifier to put converted bytes. Note that only one qualifier is supported for a converted value.")
//                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
