package com.xiaohongshu.db.hercules.kafka.mr;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManagerInitializer;
import org.apache.hadoop.mapreduce.Job;

public class KafkaOutputMRJobContext implements MRJobContext, KafkaManagerInitializer {
    @Override
    public void configureJob(Job job, WrappingOptions options) {

    }

    @Override
    public void preRun(WrappingOptions options) {

    }

    @Override
    public void postRun(WrappingOptions options) {

    }

    @Override
    public KafkaManager initializeManager(GenericOptions options) {
        return new KafkaManager(options);
    }
}
