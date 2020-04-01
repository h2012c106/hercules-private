package com.xiaohongshu.db.hercules.rdbms.output.mr;

import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RDBMSOutputFormat extends OutputFormat<NullWritable, HerculesWritable> {
    @Override
    public RecordWriter<NullWritable, HerculesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        RDBMSSchemaFetcher schemaFetcher = SchemaFetcherFactory.getSchemaFetcher(options.getSourceOptions(),
                RDBMSSchemaFetcher.class);

        if (options.getTargetOptions().hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)) {
            List<String> updateKeyList = Arrays.asList(options.getTargetOptions().getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null));
            if (!schemaFetcher.getColumnNameList().containsAll(updateKeyList)) {
                throw new IOException("The update key must be the subset of columns.");
            }
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }
}
