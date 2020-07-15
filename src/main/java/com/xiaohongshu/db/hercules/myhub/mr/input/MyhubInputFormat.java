package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.xiaohongshu.db.hercules.mysql.mr.MysqlInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public class MyhubInputFormat extends MysqlInputFormat {
    @Override
    protected List<InputSplit> innerGetSplits(JobContext context, int numSplits) throws IOException, InterruptedException {
        return super.innerGetSplits(context, numSplits);
    }
}
