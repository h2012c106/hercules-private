package com.xiaohongshu.db.hercules.core.mr.context;

import org.apache.hadoop.mapreduce.Job;

public interface MRJobContext {
    public void configureJob(Job job);

    public void preRun();

    public void postRun();
}
