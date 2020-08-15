package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

public class HerculesSerDerRecordReader extends RecordReader<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter, InjectedClass {

    private final KvSerDer<?, ?> serDer;
    private final HerculesRecordReader<?> reader;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    public HerculesSerDerRecordReader(KvSerDer<?, ?> serDer, HerculesRecordReader<?> reader) {
        this.serDer = serDer;
        this.reader = reader;
    }

    @Override
    public void afterInject() {
        String keyName = options.getString(KEY_NAME, null);
        String valueName = options.getString(VALUE_NAME, null);
        // 用了serder的必然是kv，是kv必然是这么配置参数的
        if (keyName == null || valueName == null) {
            throw new RuntimeException("Must use kv options to config to use serder.");
        }
        serDer.setKeyName(keyName);
        serDer.setValueName(valueName);
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
