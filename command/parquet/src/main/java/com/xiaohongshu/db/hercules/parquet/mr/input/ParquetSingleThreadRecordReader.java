package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;

public class ParquetSingleThreadRecordReader extends ParquetRecordReader {
    public ParquetSingleThreadRecordReader(TaskAttemptContext context, ExampleInputFormat delegateInputFormat) {
        super(context, delegateInputFormat);
    }

    @Override
    public boolean innerNextKeyValue() throws IOException, InterruptedException {
        return myNextKeyValue();
    }

    @Override
    public HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException {
        try {
            return new HerculesWritable(((ParquetInputWrapperManager) wrapperGetterFactory).groupToMapWrapper(delegate.getCurrentValue(), null));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
