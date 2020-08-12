package com.xiaohongshu.db.hercules.mongodb.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;

import java.io.IOException;

public class MongoDBOutputFormat extends HerculesOutputFormat<Document> {

    @SchemaInfo(role = DataSourceRole.TARGET)
    private Schema schema;

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
        return new MongoDBWrapperSetterManager(schema);
    }
}
