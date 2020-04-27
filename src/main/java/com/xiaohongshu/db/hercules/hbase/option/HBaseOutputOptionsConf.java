package com.xiaohongshu.db.hercules.hbase.option;

import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.List;

public class HBaseOutputOptionsConf extends HBaseOptionsConf {

    public final static String COLUMN_FAMILY = "hbase.mapreduce.column.family";
    public final static String OUTPU_TABLE = "hbase.mapreduce.outputtable";

    // define recordWriter Write thread Num
    public static final String EXECUTE_THREAD_NUM = "execute-thread-num";
    public static final Integer DEFAULT_EXECUTE_THREAD_NUM = 1;

    // puts batch size
    public static final String PUT_BATCH_SIZE = "put-batch-size";
    public static final int DEFAULT_PUT_BATCH_SIZE = 100;

    // the column specified to be the row key of PUT or DELETE operations
    // 当上游也是HBase的时候，此设置共享。
    public static final String ROW_KEY_COL_NAME = "hbase.mapreduce.rowkeycolname";
    public static final String WRITE_BUFFER_SIZE = "hbase.mapreduce.writebuffersize";
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 8 * 1024 * 1024;

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN_FAMILY)
                .needArg(true)
                .necessary(true)
                .description("Column Family to Scan.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(OUTPU_TABLE)
                .needArg(true)
                .necessary(true)
                .description("Job parameter that specifies the output table..")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EXECUTE_THREAD_NUM)
                .needArg(true)
                .description("The thread number for executing Hbase Puts.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PUT_BATCH_SIZE)
                .needArg(true)
                .description("The put batch size for each batch put.")
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
}
