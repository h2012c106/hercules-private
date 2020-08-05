package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.serializer.KvSerializer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class HerculesSerializerRecordWriter extends RecordWriter<NullWritable, HerculesWritable> {

    private final KvSerializer<?, ?> serializer;
    private final HerculesRecordWriter<?> writer;

    public HerculesSerializerRecordWriter(KvSerializer<?, ?> serializer, HerculesRecordWriter<?> writer) {
        this.serializer = serializer;
        this.writer = writer;
    }

    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        writer.write(key, serializer.write(value));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close(context);
    }

}
