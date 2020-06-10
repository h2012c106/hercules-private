package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf.TASK_SIDE_METADATA;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;

public class ParquetInputMRJobContext implements MRJobContext {

    /**
     * 在这里配置和在InputFormat里配置其实一样，但是在InputFormat里配置要等到调用的时候才能拿到options去配置而不是初始化的时候配置，觉得怪怪的。
     *
     * @param job
     * @param options
     */
    @Override
    public void configureJob(Job job, WrappingOptions options) {
        ExampleInputFormat.setTaskSideMetaData(job, options.getSourceOptions().getBoolean(TASK_SIDE_METADATA, false));
        try {
            ExampleInputFormat.addInputPath(job, new Path(options.getSourceOptions().getString(DIR, null)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preRun(WrappingOptions options) {
    }

    @Override
    public void postRun(WrappingOptions options) {
    }
}
