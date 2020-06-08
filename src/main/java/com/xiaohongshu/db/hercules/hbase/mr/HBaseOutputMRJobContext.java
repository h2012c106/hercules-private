package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;
import org.apache.hadoop.mapreduce.Job;

public class HBaseOutputMRJobContext implements MRJobContext, HBaseManagerInitializer {

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }

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
