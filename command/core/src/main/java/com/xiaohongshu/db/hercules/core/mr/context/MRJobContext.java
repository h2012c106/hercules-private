package com.xiaohongshu.db.hercules.core.mr.context;

import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import org.apache.hadoop.mapreduce.Job;

public interface MRJobContext {
    public void configureJob(Job job);

    public void preRun();

    public void postRun();
}
