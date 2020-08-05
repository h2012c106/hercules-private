package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
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

/**
 * @param <T> 数据源读入时用于表示一行的数据结构，详情可见{@link WrapperGetter}
 */
public abstract class HerculesRecordReader<T> extends RecordReader<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordReader.class);

    private long time = 0;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected WrapperGetterFactory<T> wrapperGetterFactory;

    protected WrappingOptions options;

    private Schema schema;

    protected CustomDataTypeManager<?, ?> manager;

    public HerculesRecordReader(TaskAttemptContext context) {
        options = HerculesContext.getWrappingOptions();

        schema = HerculesContext.getSchemaPair().getSourceItem();

        manager = HerculesContext.getAssemblySupplierPair().getSourceItem().getCustomDataTypeManager();
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
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

    abstract protected void myInitialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
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
        long start = System.currentTimeMillis();
        boolean res = innerNextKeyValue();
        time += (System.currentTimeMillis() - start);
        return res;
    }

    abstract public void innerClose() throws IOException;

    @Override
    public final void close() throws IOException {
        if (!closed.getAndSet(true)) {
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
