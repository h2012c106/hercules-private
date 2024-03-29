package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.PreparedStatement;

public class RDBMSOutputFormat extends HerculesOutputFormat<PreparedStatement> {

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @Assembly(role = DataSourceRole.TARGET)
    private RDBMSManager manager;

    @Override
    public RDBMSRecordWriter innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            String tableName = targetOptions.getString(RDBMSOptionsConf.TABLE, null);
            ExportType exportType = ExportType.valueOfIgnoreCase(targetOptions
                    .getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null));
            if (targetOptions.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
                tableName = targetOptions.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
                if (targetOptions.getBoolean(RDBMSOutputOptionsConf.CLOSE_FORCE_INSERT_STAGING, false)) {
                    exportType = ExportType.INSERT;
                }
            }

            if (ExportType.valueOfIgnoreCase(targetOptions.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate()) {
                return new RDBMSUpdateRecordWriter(context, tableName, exportType);
            }
            if (targetOptions.getBoolean(RDBMSOutputOptionsConf.BATCH, false)) {
                return new RDBMSBatchRecordWriter(context, tableName, exportType);
            } else {
                return new RDBMSRichLineRecordWriter(context, tableName, exportType);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void innerCheckOutputSpecs(JobContext context) throws IOException, InterruptedException {
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
    protected RDBMSWrapperSetterFactory createWrapperSetterFactory() {
        return new RDBMSWrapperSetterFactory();
    }
}
