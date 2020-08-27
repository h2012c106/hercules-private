package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import org.apache.hadoop.mapreduce.Job;

public class HBaseInputMRJobContext extends BaseMRJobContext {
    public HBaseInputMRJobContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void configureJob(Job job) {
        HBaseManager.setBasicConf(job.getConfiguration(), getOptions());
    }

    @Override
    public void preRun() {

    }

    @Override
    public void postRun() {

    }
}
