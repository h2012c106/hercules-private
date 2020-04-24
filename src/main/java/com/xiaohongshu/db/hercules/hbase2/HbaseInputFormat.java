package com.xiaohongshu.db.hercules.hbase2;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.IOException;
import java.util.List;

public class HbaseInputFormat extends HerculesInputFormat {


    private HbaseTableInputFormat hbaseTableInputFormat = new HbaseTableInputFormat();

    @Override
    protected List<InputSplit> innerGetSplits(JobContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return hbaseTableInputFormat.getSplits(context);
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return hbaseTableInputFormat.createRecordReader(split, context);
    }

    @Override
    protected HerculesRecordReader innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public DataTypeConverter initializeConverter() {
        return null;
    }
}
