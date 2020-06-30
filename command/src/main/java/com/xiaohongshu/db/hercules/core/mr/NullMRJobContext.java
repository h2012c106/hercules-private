package com.xiaohongshu.db.hercules.core.mr;

import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import org.apache.hadoop.mapreduce.Job;

public class NullMRJobContext implements MRJobContext {

    public static final MRJobContext INSTANCE = new NullMRJobContext();

    @Override
    public void configureJob(Job job, WrappingOptions options) {

    }

    @Override
    public void preRun(WrappingOptions options) {

    }

    @Override
    public void postRun(WrappingOptions options) {

    }
}
