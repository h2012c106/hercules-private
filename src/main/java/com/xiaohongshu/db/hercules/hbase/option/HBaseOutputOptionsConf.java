package com.xiaohongshu.db.hercules.hbase.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.*;

import java.util.ArrayList;
import java.util.List;

public final class HBaseOutputOptionsConf extends BaseOptionsConf {

    public final static String COLUMN_FAMILY = "hbase.mapreduce.column.family";

    // define recordWriter Write thread Num
    public static final String EXECUTE_THREAD_NUM = "execute-thread-num";
    public static final Integer DEFAULT_EXECUTE_THREAD_NUM = 1;

    // the column specified to be the row key of PUT or DELETE operations
    // 当上游也是HBase的时候，此设置共享。
    public static final String ROW_KEY_COL_NAME = "hbase.mapreduce.rowkeycolname";
    public static final String WRITE_BUFFER_SIZE = "hbase.mapreduce.writebuffersize";
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

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
                .name(EXECUTE_THREAD_NUM)
                .needArg(true)
                .description("The thread number for executing Hbase Puts.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(ROW_KEY_COL_NAME)
                .needArg(true)
                .necessary(true)
                .description("The column specified to be the row key of PUT or DELETE operations")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(WRITE_BUFFER_SIZE)
                .needArg(true)
                .description(String.format("The write buffer size, default %d bytes.", DEFAULT_EXECUTE_THREAD_NUM))
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
