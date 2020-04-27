package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.schema.HBaseDataTypeConverter;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * 用 proxy 的方式，使用 TableInputFormat 来访问上游数据库，实现 split 和 recordReader 功能。
 */
public class HBaseInputFormat extends HerculesInputFormat<HBaseDataTypeConverter> implements HBaseManagerInitializer {

    private HBaseTableInputFormat hbaseTableInputFormat;
    private HBaseManager manager;

    @Override
    protected void initializeContext(GenericOptions sourceOptions) {
        super.initializeContext(sourceOptions);

        manager = initializeManager(sourceOptions);
        hbaseTableInputFormat = new HBaseTableInputFormat(manager, converter);
    }

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException {
        return hbaseTableInputFormat.getSplits(context);
    }

    @Override
    protected HerculesRecordReader innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return hbaseTableInputFormat.createRecordReader(split, context);
    }

    @Override
    public HBaseDataTypeConverter initializeConverter() {
        return new HBaseDataTypeConverter();
    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}
