package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.xiaohongshu.db.hercules.core.mr.mapper.HerculesMapper.HERCULES_GROUP_NAME;

/**
 * @param <T> 数据源读入时用于表示一行的数据结构，详情可见{@link WrapperGetter}
 */
public abstract class HerculesRecordReader<T> extends RecordReader<NullWritable, HerculesWritable>
        implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(HerculesRecordReader.class);

    public static final String READ_RECORDS_COUNTER_NAME = "Read records num";

    private long time = 0;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected WrapperGetterFactory<T> wrapperGetterFactory;

    private Object filter = null;

    private TaskAttemptContext context;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema schema;

    public HerculesRecordReader(TaskAttemptContext context) {
    }

    protected Object getFilter() {
        return filter;
    }

    public void setFilter(Object filter) {
        this.filter = filter;
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SOURCE;
    }

    public final void setWrapperGetterFactory(@NonNull WrapperGetterFactory<T> wrapperGetterFactory) {
        this.wrapperGetterFactory = wrapperGetterFactory;

        if (wrapperGetterFactory != null) {
            // 检查column type里是否有不支持的类型
            Map<String, DataType> errorMap = new HashMap<>();
            for (Map.Entry<String, DataType> entry : schema.getColumnTypeMap().entrySet()) {
                String columnName = entry.getKey();
                DataType dataType = entry.getValue();
                if (!dataType.isCustom() && !wrapperGetterFactory.contains(dataType)) {
                    errorMap.put(columnName, dataType);
                }
            }
            if (errorMap.size() > 0) {
                throw new RuntimeException("Unsupported read base data types: " + errorMap);
            }
        }
    }

    protected void afterSetWrapperGetterFactory() {
    }

    abstract protected void myInitialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.context = context;

        long start = System.currentTimeMillis();
        myInitialize(split, context);
        time += (System.currentTimeMillis() - start);
    }

    @Override
    public final NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    abstract protected HerculesWritable innerGetCurrentValue() throws IOException, InterruptedException;

    @Override
    public final HerculesWritable getCurrentValue() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        HerculesWritable res = innerGetCurrentValue();
        time += (System.currentTimeMillis() - start);
        return res;
    }

    abstract public boolean innerNextKeyValue() throws IOException, InterruptedException;

    @Override
    public final boolean nextKeyValue() throws IOException, InterruptedException {
        context.getCounter(HERCULES_GROUP_NAME, READ_RECORDS_COUNTER_NAME).increment(1L);

        long start = System.currentTimeMillis();
        boolean res = innerNextKeyValue();
        time += (System.currentTimeMillis() - start);
        return res;
    }

    abstract public void innerClose() throws IOException;

    @Override
    public final void close() throws IOException {
        if (!closed.getAndSet(true)) {
            // 最后一个nextKeyValue会返回false
            context.getCounter(HERCULES_GROUP_NAME, READ_RECORDS_COUNTER_NAME).increment(-1L);

            long start = System.currentTimeMillis();
            innerClose();
            time += (System.currentTimeMillis() - start);
            LOG.info(String.format("Spent %.3fs on read.", (double) time / 1000.0));
        }
    }

    protected final WrapperGetter<T> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }
}
