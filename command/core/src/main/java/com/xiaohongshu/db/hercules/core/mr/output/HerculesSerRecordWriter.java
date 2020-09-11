package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

public class HerculesSerRecordWriter extends RecordWriter<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter, InjectedClass {

    private final KVSer<?> ser;
    private final HerculesRecordWriter<?> writer;

    @Options(type = OptionsType.TARGET)
    private GenericOptions options;

    private final TaskAttemptContext context;

    public HerculesSerRecordWriter(KVSer<?> ser, HerculesRecordWriter<?> writer, TaskAttemptContext context) {
        this.ser = ser;
        this.writer = writer;
        this.context = context;
    }

    @Override
    public void afterInject() {
        String keyName = options.getString(KEY_NAME, null);
        String valueName = options.getString(VALUE_NAME, null);
        // 用了serder的必然是kv，是kv必然是这么配置参数的
        if (keyName == null || valueName == null) {
            throw new RuntimeException("Must use kv options to config to use serder.");
        }
        ser.setKeyName(keyName);
        ser.setValueName(valueName);
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.TARGET;
    }

    @Override
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        HerculesStatus.increase(context, HerculesCounter.SER_RECORDS);
        value = ser.write(value);
        // 如果ser返回null，则不写这行
        if (value != null) {
            HerculesStatus.increase(context, HerculesCounter.SER_ACTUAL_RECORDS);
            writer.write(key, value);
        } else {
            HerculesStatus.increase(context, HerculesCounter.SER_IGNORE_RECORDS);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        ser.close(context);
        writer.close(context);
    }

}
