package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HerculesSerDerRecordWriter extends RecordWriter<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter {

    private final KvSerDer<?, ?> serDer;
    private final HerculesRecordWriter<?> writer;

    public HerculesSerDerRecordWriter(KvSerDer<?, ?> serDer, HerculesRecordWriter<?> writer) {
        this.serDer = serDer;
        this.writer = writer;
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.TARGET;
    }

    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        writer.write(key, serDer.write(value));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close(context);
    }

}
