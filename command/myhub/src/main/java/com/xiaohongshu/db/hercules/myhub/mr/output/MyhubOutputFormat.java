package com.xiaohongshu.db.hercules.myhub.mr.output;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSRecordWriter;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class MyhubOutputFormat extends RDBMSOutputFormat {

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @Override
    public RDBMSRecordWriter innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            String tableName = targetOptions.getString(RDBMSOptionsConf.TABLE, null);
            ExportType exportType = ExportType.valueOfIgnoreCase(targetOptions
                    .getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null));

            if (ExportType.valueOfIgnoreCase(targetOptions.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate()) {
                throw new UnsupportedEncodingException();
            } else {
                return new MyhubRecordWriter(context, tableName, exportType);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
