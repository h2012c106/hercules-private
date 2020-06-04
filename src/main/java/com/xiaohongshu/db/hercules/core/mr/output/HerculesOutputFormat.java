package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManagerGenerator;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public abstract class HerculesOutputFormat extends OutputFormat<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesOutputFormat.class);

    public HerculesOutputFormat() {
    }

    @Override
    abstract public HerculesRecordWriter<?> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException;
}
