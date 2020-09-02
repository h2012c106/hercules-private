package com.xiaohongshu.db.hercules.core.mr.context;

import org.apache.hadoop.mapreduce.Job;

public class NullMRJobContext implements MRJobContext {

    public static final MRJobContext INSTANCE = new NullMRJobContext();

    @Override
    public void configureJob(Job job) {

    }

    @Override
    public void preRun() {

    }

    @Override
    public void postRun() {

    }
}
