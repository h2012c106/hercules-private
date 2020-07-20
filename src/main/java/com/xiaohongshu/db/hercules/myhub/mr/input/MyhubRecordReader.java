package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.StingyMap;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSInputSplit;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSRecordReader;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class MyhubRecordReader extends RDBMSRecordReader {

    public MyhubRecordReader(TaskAttemptContext context, RDBMSManager manager) {
        super(context, manager);
    }

    @Override
    protected String makeSql(GenericOptions sourceOptions, InputSplit split) {
        String querySql = SqlUtils.makeBaseQuery(sourceOptions);
        return String.format(
                "/*MYHUB SHARD_NODES:%s; SLAVE_PREFER */",
                ((MyhubInputSplit) split)
                        .getShardSeqList()
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","))
        ) + querySql;
    }

}
