package com.xiaohongshu.db.hercules.core.mr.output;

import com.google.common.util.concurrent.RateLimiter;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import lombok.NonNull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @param <T> 数据源写出时用于表示一行的数据结构，详情可见{@link WrapperSetter}
 */
public abstract class HerculesRecordWriter<T> extends RecordWriter<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected WrappingOptions options;
    private Map<DataType, WrapperSetter<T>> wrapperSetterMap;

    protected List<String> columnNameList;
    protected Map<String, DataType> columnTypeMap;

    protected RateLimiter rateLimiter = null;
    private double acquireTime = 0;

    private boolean emptyColumnNameList;

    private void setWrapperSetter(Map<DataType, WrapperSetter<T>> wrapperSetterMap,
                                  DataType dataType, Function<Void, WrapperSetter<T>> setFunction) {
        try {
            WrapperSetter<T> tmpWrapper = setFunction.apply(null);
            if (tmpWrapper != null) {
                wrapperSetterMap.put(dataType, tmpWrapper);
            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            LOG.warn(String.format("Undefined convert strategy of %s, exception: %s",
                    dataType.toString(),
                    ExceptionUtils.getStackTrace(e)));
        }
    }

    private void initializeWrapperSetterMap() {
        wrapperSetterMap = new HashMap<>(DataType.values().length);
        for (DataType dataType : DataType.values()) {
            switch (dataType) {
                case INTEGER:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getIntegerSetter();
                        }
                    });
                    break;
                case DOUBLE:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getDoubleSetter();
                        }
                    });
                    break;
                case BOOLEAN:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getBooleanSetter();
                        }
                    });
                    break;
                case STRING:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getStringSetter();
                        }
                    });
                    break;
                case DATE:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getDateSetter();
                        }
                    });
                    break;
                case BYTES:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getBytesSetter();
                        }
                    });
                    break;
                case NULL:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getNullSetter();
                        }
                    });
                    break;
                case LIST:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getListSetter();
                        }
                    });
                    break;
                case MAP:
                    setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                        @Override
                        public WrapperSetter<T> apply(Void aVoid) {
                            return getMapSetter();
                        }
                    });
                    break;
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        }
    }

    public HerculesRecordWriter(TaskAttemptContext context) {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        LOG.warn(context.getConfiguration().get("hercules.column.target","sadasdsad"));

        columnNameList = Arrays.asList(options.getTargetOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        columnTypeMap = SchemaUtils.convert(options.getTargetOptions().getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));

        initializeWrapperSetterMap();

        emptyColumnNameList = columnNameList.size() == 0;

        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            rateLimiter = RateLimiter.create(options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null));
        }
    }

    protected final WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        WrapperSetter<T> res = wrapperSetterMap.get(dataType);
        if (res == null) {
            throw new MapReduceException("Unknown data type: " + dataType.name());
        } else {
            return res;
        }
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

    abstract protected WrapperSetter<T> getIntegerSetter();

    abstract protected WrapperSetter<T> getDoubleSetter();

    abstract protected WrapperSetter<T> getBooleanSetter();

    abstract protected WrapperSetter<T> getStringSetter();

    abstract protected WrapperSetter<T> getDateSetter();

    abstract protected WrapperSetter<T> getBytesSetter();

    abstract protected WrapperSetter<T> getNullSetter();

    protected WrapperSetter<T> getListSetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperSetter<T> getMapSetter() {
        throw new UnsupportedOperationException();
    }

}
