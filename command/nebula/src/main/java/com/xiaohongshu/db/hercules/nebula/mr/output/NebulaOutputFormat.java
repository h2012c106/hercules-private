package com.xiaohongshu.db.hercules.nebula.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesRecordWriter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.nebula.WritingRow;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class NebulaOutputFormat extends HerculesOutputFormat<WritingRow> {
    @Override
    protected HerculesRecordWriter<WritingRow> innerGetRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NebulaRecordWriter(context);
    }

    @Override
    protected WrapperSetterFactory<WritingRow> createWrapperSetterFactory() {
        return new NebulaWrapperSetterManager();
    }
}
