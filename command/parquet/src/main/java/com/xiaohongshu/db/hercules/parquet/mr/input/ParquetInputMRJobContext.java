package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetInputOptionsConf.TASK_SIDE_METADATA;
import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.DIR;

public class ParquetInputMRJobContext extends BaseMRJobContext {

    public ParquetInputMRJobContext(GenericOptions options) {
        super(options);
    }

    /**
     * 在这里配置和在InputFormat里配置其实一样，但是在InputFormat里配置要等到调用的时候才能拿到options去配置而不是初始化的时候配置，觉得怪怪的。
     *
     * @param job
     */
    @Override
    public void configureJob(Job job) {
        ExampleInputFormat.setTaskSideMetaData(job, getOptions().getBoolean(TASK_SIDE_METADATA, false));
        try {
            ExampleInputFormat.addInputPath(job, new Path(getOptions().getString(DIR, null)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preRun() {
    }

    @Override
    public void postRun() {
    }
}
