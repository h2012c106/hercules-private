package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;

import java.io.IOException;

public class ParquetOutputFormat extends HerculesOutputFormat {

    private final ExampleOutputFormat delegate = new ExampleOutputFormat();

    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getTargetOptions().getString(ParquetOptionsConf.SCHEMA_STYLE, null));
        ParquetOutputWrapperManager wrapperManager;
        switch (schemaStyle) {
            case SQOOP:
                wrapperManager = new ParquetSqoopOutputWrapperManager();
                break;
            case HIVE:
                wrapperManager = new ParquetHiveOutputWrapperManager();
                break;
            case ORIGINAL:
                wrapperManager = new ParquetHerculesOutputWrapperManager();
                break;
            default:
                throw new RuntimeException();
        }

        return new ParquetRecordWriter(context, delegate.getRecordWriter(context), wrapperManager);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        delegate.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return delegate.getOutputCommitter(context);
    }
}
