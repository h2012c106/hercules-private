package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.schema.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetSchemaOutputFormat extends HerculesOutputFormat<TypeBuilderTreeNode> {

    private final TextOutputFormat<NullWritable, Text> delegate = new TextOutputFormat<>();

    @Override
    public HerculesRecordWriter<TypeBuilderTreeNode> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetSchemaRecordWriter(context, delegate.getRecordWriter(context));
    }

    @Override
    protected WrapperSetterFactory<TypeBuilderTreeNode> createWrapperSetterFactory() {
        return new ParquetSchemaOutputWrapperManager();
    }

    @Override
    public void innerCheckOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        delegate.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter innerGetOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return delegate.getOutputCommitter(taskAttemptContext);
    }
}
