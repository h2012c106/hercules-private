package com.xiaohongshu.db.hercules.core.mr;

import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import org.apache.hadoop.mapreduce.Job;

public interface MRJobContext {
    public void configureJob(Job job, WrappingOptions options);

    public void preRun(WrappingOptions options);

    public void postRun(WrappingOptions options);
}
