package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManagerGenerator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MongoDBOutputFormat extends HerculesOutputFormat implements MongoDBManagerGenerator {
    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        GenericOptions targetOptions = options.getTargetOptions();
        try {
            return new MongoDBRecordWriter(context, generateManager(targetOptions));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    @Override
    public MongoDBManager generateManager(GenericOptions options) {
        return new MongoDBManager(options);
    }
}
