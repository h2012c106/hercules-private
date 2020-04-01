package com.xiaohongshu.db.hercules.rdbms.output.mr;

import com.cloudera.sqoop.mapreduce.NullOutputCommitter;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class RDBMSOutputFormat extends HerculesOutputFormat<RDBMSSchemaFetcher> {
    @Override
    public HerculesRecordWriter<?, RDBMSSchemaFetcher> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        GenericOptions targetOptions = options.getTargetOptions();

        RDBMSSchemaFetcher schemaFetcher = getSchemaFetcher(targetOptions);

        try {
            String tableName = targetOptions.getString(RDBMSOptionsConf.TABLE, null);
            ExportType exportType = ExportType.valueOfIgnoreCase(options.getTargetOptions()
                    .getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null));
            if (targetOptions.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
                tableName = targetOptions.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
                if (targetOptions.getBoolean(RDBMSOutputOptionsConf.CLOSE_FORCE_INSERT_STAGING, false)) {
                    exportType = ExportType.INSERT;
                }
            }

            if (ExportType.valueOfIgnoreCase(targetOptions.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate()) {
                return new RDBMSUpdateRecordWriter(context, tableName, exportType, schemaFetcher);
            }
            if (targetOptions.getBoolean(RDBMSOutputOptionsConf.BATCH, false)) {
                return new RDBMSBatchRecordWriter(context, tableName, exportType, schemaFetcher);
            } else {
                return new RDBMSRichLineRecordWriter(context, tableName, exportType, schemaFetcher);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
//        Configuration configuration = context.getConfiguration();
//
//        WrappingOptions options = new WrappingOptions();
//        options.fromConfiguration(configuration);
//
//        RDBMSSchemaFetcher schemaFetcher = SchemaFetcherFactory.getSchemaFetcher(options.getSourceOptions(),
//                RDBMSSchemaFetcher.class);
//
//        // 不然fetcher的columnNameTypeMap当中根本找不到update key的类型信息
//        if (options.getTargetOptions().hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)) {
//            List<String> updateKeyList = Arrays.asList(options.getTargetOptions().getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null));
//            if (!schemaFetcher.getColumnNameList().containsAll(updateKeyList)) {
//                throw new IOException("The update key must be the subset of columns.");
//            }
//        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NullOutputCommitter();
    }

    @Override
    public RDBMSSchemaFetcher innerGetSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, RDBMSSchemaFetcher.class);
    }
}
