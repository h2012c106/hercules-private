package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

public class MongoDBOutputFormat extends HerculesOutputFormat<Document> {

    @Override
    public HerculesRecordWriter<Document> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new MongoDBRecordWriter(context);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected WrapperSetterFactory<Document> createWrapperSetterFactory() {
        return new MongoDBWrapperSetterManager();
    }
}
