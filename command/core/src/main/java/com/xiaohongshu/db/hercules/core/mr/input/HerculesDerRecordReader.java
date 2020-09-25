package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serder.KVDer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.KEY_NAME;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.VALUE_NAME;

/**
 * 得要支持某种序列化结构中包含多行的情况
 */
public class HerculesDerRecordReader extends RecordReader<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter, InjectedClass {

    private final KVDer<?> der;
    private final HerculesRecordReader<?> reader;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions options;

    private TaskAttemptContext context;

    /**
     * 模仿迭代器头指针
     */
    private static final int SEQ_BEGIN = -1;
    private List<HerculesWritable> derRes = Collections.emptyList();
    private int derResSeq = SEQ_BEGIN;

    public HerculesDerRecordReader(KVDer<?> der, HerculesRecordReader<?> reader) {
        this.der = der;
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
        der.setKeyName(keyName);
        der.setValueName(valueName);
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.context = context;
        reader.initialize(split, context);
    }

    /**
     * 这里不能用递归，因为这里的场景有可能导致stackoverflow
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean first = true;
        do {
            while (CollectionUtils.isEmpty(derRes) || ++derResSeq >= derRes.size()) {
                if (!reader.nextKeyValue()) {
                    return false;
                }
                derRes = der.read(reader.getCurrentValue());
                derResSeq = SEQ_BEGIN;
            }
            HerculesStatus.increase(context, HerculesCounter.DER_RECORDS);
            // 记录因为缺k或v导致返回null的数目
            if (first) {
                first = false;
            } else {
                HerculesStatus.increase(context, HerculesCounter.DER_IGNORE_RECORDS);
            }
        } while (getCurrentValue() == null);
        HerculesStatus.increase(context, HerculesCounter.DER_ACTUAL_RECORDS);
        return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public HerculesWritable getCurrentValue() throws IOException, InterruptedException {
        return derRes.get(derResSeq);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        der.close();
        reader.close();
    }
}
