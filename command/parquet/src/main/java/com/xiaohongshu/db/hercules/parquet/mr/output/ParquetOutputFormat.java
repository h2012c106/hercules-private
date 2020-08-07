package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetOutputFormat extends HerculesOutputFormat<Group> {

    private final ExampleOutputFormat delegate = new ExampleOutputFormat();

    @Override
    public HerculesRecordWriter<Group> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetRecordWriter(context, delegate.getRecordWriter(context));
    }

    @Override
    protected ParquetOutputWrapperManager createWrapperSetterFactory() {
        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(HerculesContext.getWrappingOptions().getTargetOptions().getString(SCHEMA_STYLE, null));
        switch (schemaStyle) {
            case SQOOP:
                return new ParquetSqoopOutputWrapperManager();
            case HIVE:
                return new ParquetHiveOutputWrapperManager();
            case ORIGINAL:
                return new ParquetHerculesOutputWrapperManager();
            default:
                throw new RuntimeException();
        }
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
