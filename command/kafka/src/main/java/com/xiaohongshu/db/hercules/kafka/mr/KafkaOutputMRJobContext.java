package com.xiaohongshu.db.hercules.kafka.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import com.xiaohongshu.db.hercules.kafka.schema.manager.KafkaManager;
import org.apache.hadoop.mapreduce.Job;

public class KafkaOutputMRJobContext extends BaseMRJobContext {

    @Assembly(role = DataSourceRole.TARGET)
    private KafkaManager kafkaManager;

    public KafkaOutputMRJobContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void configureJob(Job job) {
    }

    @Override
    public void preRun() {
        if (getOptions().getBoolean(KafkaOptionConf.DELETE_BEFORE_RUN, KafkaOptionConf.DEFAULT_DELETE_BEFORE_RUN)) {
            kafkaManager.deleteRecordsByTopicBeforeRun();
        }
    }

    @Override
    public void postRun() {

    }
}
