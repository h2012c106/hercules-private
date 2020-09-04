package com.xiaohongshu.db.hercules.core.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.mr.mapper.HerculesMapper.HERCULES_GROUP_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

public class HerculesSerRecordWriter extends RecordWriter<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter, InjectedClass {

    public static final String SER_RECORDS_COUNTER_NAME = "Serialize records num";
    public static final String SER_IGNORE_RECORDS_COUNTER_NAME = "Serialize ignored records num (missing key or value)";
    public static final String SER_ACTUAL_RECORDS_COUNTER_NAME = "Serialize actual records num";

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
        context.getCounter(HERCULES_GROUP_NAME, SER_RECORDS_COUNTER_NAME).increment(1L);
        value = ser.write(value);
        // 如果ser返回null，则不写这行
        if (value != null) {
            context.getCounter(HERCULES_GROUP_NAME, SER_ACTUAL_RECORDS_COUNTER_NAME).increment(1L);
            writer.write(key, value);
        } else {
            context.getCounter(HERCULES_GROUP_NAME, SER_IGNORE_RECORDS_COUNTER_NAME).increment(1L);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        ser.close(context);
        writer.close(context);
    }

}
