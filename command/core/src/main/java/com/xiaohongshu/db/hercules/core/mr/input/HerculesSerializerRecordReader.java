package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HerculesSerializerRecordReader extends RecordReader<NullWritable, HerculesWritable> {

    private final KvSerializer<?, ?> serializer;
    private final HerculesRecordReader<?> reader;

    public HerculesSerializerRecordReader(KvSerializer<?, ?> serializer, HerculesRecordReader<?> reader) {
        this.serializer = serializer;
        this.reader = reader;
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
        return serializer.read(reader.getCurrentValue());
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
