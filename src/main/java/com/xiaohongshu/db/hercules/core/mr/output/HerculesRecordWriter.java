package com.xiaohongshu.db.hercules.core.mr.output;

import com.google.common.util.concurrent.RateLimiter;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @param <T> 数据源写出时用于表示一行的数据结构，详情可见{@link WrapperSetter}
 */
public abstract class HerculesRecordWriter<T> extends RecordWriter<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    private WrapperSetterFactory<T> wrapperSetterFactory;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected WrappingOptions options;

    protected List<String> columnNameList;
    protected Map<String, DataType> columnTypeMap;

    private RateLimiter rateLimiter = null;
    private double acquireTime = 0;

    private boolean emptyColumnNameList;

    public HerculesRecordWriter(TaskAttemptContext context, WrapperSetterFactory<T> wrapperSetterFactory) {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        LOG.warn(context.getConfiguration().get("hercules.column.target", "sadasdsad"));

        columnNameList = Arrays.asList(options.getTargetOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        columnTypeMap = SchemaUtils.convert(options.getTargetOptions().getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));

        this.wrapperSetterFactory = wrapperSetterFactory;

        emptyColumnNameList = columnNameList.size() == 0;

        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            rateLimiter = RateLimiter.create(options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null));
        }
    }

    /**
     * 子类有可能会有内部非静态类（如mongo），初始化无法new出来，留个口子
     *
     * @param wrapperSetterFactory
     */
    protected void setWrapperSetterFactory(WrapperSetterFactory<T> wrapperSetterFactory) {
        this.wrapperSetterFactory = wrapperSetterFactory;
    }

    protected final WrapperSetter<T> getWrapperSetter(DataType dataType) {
        return wrapperSetterFactory.getWrapperSetter(dataType);
    }

    /**
     * 当writer可以得到column列表时，逐column一一对应写入
     *
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    abstract protected void innerColumnWrite(HerculesWritable value) throws IOException, InterruptedException;

    /**
     * 当writer拿不到column列表时，整个map往里写
     *
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    abstract protected void innerMapWrite(HerculesWritable value) throws IOException, InterruptedException;

    /**
     * 即使下游是攒着批量写也没问题，在写之前一定等够了对应qps的时间，batch写一定避免不了毛刺的qps，但是batch间的qps是一定能保证的
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public final void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        if (rateLimiter != null) {
            acquireTime += rateLimiter.acquire();
        }
        if (emptyColumnNameList) {
            innerMapWrite(value);
        } else {
            innerColumnWrite(value);
        }
    }

    abstract protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException;

    @Override
    public final void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (!closed.getAndSet(true)) {
            innerClose(context);
            LOG.info(String.format("Spent %.2fs of blocking on qps control.", acquireTime));
        }
    }

}
