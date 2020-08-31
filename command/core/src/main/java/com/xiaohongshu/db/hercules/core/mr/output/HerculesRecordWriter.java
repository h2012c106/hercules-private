package com.xiaohongshu.db.hercules.core.mr.output;

import com.google.common.util.concurrent.RateLimiter;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import lombok.NonNull;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @param <T> 数据源写出时用于表示一行的数据结构，详情可见{@link WrapperSetter}
 */
public abstract class HerculesRecordWriter<T> extends RecordWriter<NullWritable, HerculesWritable>
        implements InjectedClass, DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    private long time = 0;

    protected WrapperSetterFactory<T> wrapperSetterFactory;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @SchemaInfo(role = DataSourceRole.TARGET)
    private Schema schema;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    private RateLimiter rateLimiter = null;
    private double acquireTime = 0;

    public HerculesRecordWriter(TaskAttemptContext context) {
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.TARGET;
    }

    protected void innerAfterInject() {
    }

    @Override
    public final void afterInject() {
        if (commonOptions.hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            double qps = commonOptions.getDouble(CommonOptionsConf.MAX_WRITE_QPS, null);
            LOG.info("The map max qps is limited to: " + qps);
            rateLimiter = RateLimiter.create(qps);
        }
        innerAfterInject();
    }

    /**
     * 子类有可能会有内部非静态类（如mongo），初始化无法new出来，留个口子
     *
     * @param wrapperSetterFactory
     */
    public final void setWrapperSetterFactory(@NonNull WrapperSetterFactory<T> wrapperSetterFactory) {
        this.wrapperSetterFactory = wrapperSetterFactory;

        if (wrapperSetterFactory != null) {
            // 检查column type里是否有不支持的类型
            Map<String, DataType> errorMap = new HashMap<>();
            for (Map.Entry<String, DataType> entry : schema.getColumnTypeMap().entrySet()) {
                String columnName = entry.getKey();
                DataType dataType = entry.getValue();
                if (!dataType.isCustom() && !wrapperSetterFactory.contains(dataType)) {
                    errorMap.put(columnName, dataType);
                }
            }
            if (errorMap.size() > 0) {
                throw new RuntimeException("Unsupported write base data types: " + errorMap);
            }
        }
    }

    protected void afterSetWrapperSetterFactory() {
    }

    protected final WrapperSetter<T> getWrapperSetter(DataType dataType) {
        return wrapperSetterFactory.getWrapperSetter(dataType);
    }

    /**
     * 无论下游写策略是"给什么拿什么"还是"要什么拿什么"，到这个函数里无脑"给什么拿什么"即可，在write中已经经过了补全、去多的逻辑。
     * 不用担心"要什么拿什么"的情况不进write中二次处理的逻辑，是否属于"要什么"的情况其实就是是由columnNameList决定，如果它屁都没给，何谈"要什么"
     *
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    abstract protected void innerWrite(HerculesWritable value) throws IOException, InterruptedException;

    /**
     * 给出若上游未提供某列时的策略，包括: 忽视这列、抛错、为这列置null(NullWrapper)
     * 本策略用于"要什么"的情况下，从上游提供的信息中二次提炼出下游需要的列
     * 不同策略会影响不同的写行为。
     *
     * @return
     */
    abstract protected WritableUtils.FilterUnexistOption getColumnUnexistOption();

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
        long start = System.currentTimeMillis();
        if (schema.getColumnNameList().size() != 0) {
            // TODO 此处使用copy新建了一个writable，可能在性能及垃圾回收上不友好，暂未想出更好的姿势
            value = new HerculesWritable(WritableUtils.copyColumn(value.getRow(), schema.getColumnNameList(), getColumnUnexistOption()));
        }
        innerWrite(value);
        time += (System.currentTimeMillis() - start);
    }

    abstract protected void innerClose(TaskAttemptContext context) throws IOException, InterruptedException;

    @Override
    public final void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (!closed.getAndSet(true)) {
            long start = System.currentTimeMillis();
            innerClose(context);
            time += (System.currentTimeMillis() - start);
            LOG.info(String.format("Spent %.3fs of blocking on qps control.", acquireTime));
            LOG.info(String.format("Spent %.3fs on write.", (double) time / 1000.0));
        }
    }

}
