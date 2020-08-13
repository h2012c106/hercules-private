package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HerculesSerDerRecordReader extends RecordReader<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter {

    private final KvSerDer<?, ?> serDer;
    private final HerculesRecordReader<?> reader;

    public HerculesSerDerRecordReader(KvSerDer<?, ?> serDer, HerculesRecordReader<?> reader) {
        this.serDer = serDer;
        this.reader = reader;
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {
        return serDer.read(reader.getCurrentValue());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
