package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSRecordReader;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.stream.Collectors;

public class MyhubShardRecordReader extends MyhubRecordReader {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordReader.class);

    @Assembly
    private RDBMSManager manager;

    public MyhubShardRecordReader(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected String makeSplitSql(String querySql, InputSplit split) {
        return String.format(
                "/*MYHUB SHARD_NODES:%s; SLAVE_PREFER*/",
                ((MyhubInputSplit) split)
                        .getShardSeqList()
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","))
        ) + querySql;
    }

    @Override
    protected void myInitialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        super.myInitialize(split, context);
        mapAverageRowNum *= ((MyhubInputSplit) split).getShardSeqList().size();
    }

}
