package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.mongodb.schema.manager.MongoDBManager;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

public class MongoDBOutputFormat extends HerculesOutputFormat<Document> {
    @Override
    public HerculesRecordWriter<Document> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        GenericOptions targetOptions = options.getTargetOptions();
        try {
            return new MongoDBRecordWriter(context, new MongoDBManager(targetOptions));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected WrapperSetterFactory<Document> createWrapperSetterFactory() {
        return new MongoDBWrapperSetterManager();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }
}
