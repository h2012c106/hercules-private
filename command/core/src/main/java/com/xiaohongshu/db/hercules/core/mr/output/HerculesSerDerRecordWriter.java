package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KvSerDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

public class HerculesSerDerRecordWriter extends RecordWriter<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter, InjectedClass {

    private final KvSerDer<?, ?> serDer;
    private final HerculesRecordWriter<?> writer;

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    public HerculesSerDerRecordWriter(KvSerDer<?, ?> serDer, HerculesRecordWriter<?> writer) {
        this.serDer = serDer;
        this.writer = writer;
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
