package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.parquet.SchemaStyle;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHiveDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSqoopDataTypeConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.SCHEMA_STYLE;

public class ParquetSchemaOutputFormat extends HerculesOutputFormat {

    private TextOutputFormat<NullWritable, Text> delegate = new TextOutputFormat<>();

    @Override
    public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);

        SchemaStyle schemaStyle = SchemaStyle.valueOfIgnoreCase(options.getTargetOptions().getString(SCHEMA_STYLE, null));
        ParquetDataTypeConverter converter;
        switch (schemaStyle) {
            case SQOOP:
                converter = ParquetSqoopDataTypeConverter.getInstance();
                break;
            case HIVE:
                converter = ParquetHiveDataTypeConverter.getInstance();
                break;
            case ORIGINAL:
                converter = ParquetHerculesDataTypeConverter.getInstance();
                break;
            default:
                throw new RuntimeException();
        }

        return new ParquetSchemaRecordWriter(context, new ParquetSchemaOutputWrapperManager(converter, options.getTargetOptions()),
                delegate.getRecordWriter(context));
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        delegate.checkOutputSpecs(jobContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return delegate.getOutputCommitter(taskAttemptContext);
    }
}
