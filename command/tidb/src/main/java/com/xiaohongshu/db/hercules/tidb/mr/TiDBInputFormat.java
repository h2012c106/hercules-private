package com.xiaohongshu.db.hercules.tidb.mr;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesRecordReader;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;

public class TiDBInputFormat extends RDBMSInputFormat {
    @Override
    public HerculesRecordReader<ResultSet> innerCreateRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new TiDBRecordReader(context);
    }
}
