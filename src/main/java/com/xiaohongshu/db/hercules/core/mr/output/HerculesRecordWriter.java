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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @param <T> 数据源写出时用于表示一行的数据结构，详情可见{@link WrapperSetter}
 */
public abstract class HerculesRecordWriter<T> extends RecordWriter<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    protected WrappingOptions options;
    protected List<WrapperSetter<T>> wrapperSetterList;

    protected List<String> columnNameList;
    protected Map<String, DataType> columnTypeMap;

    protected RateLimiter rateLimiter = null;

    private boolean emptyColumnNameList;

    protected List<WrapperSetter<T>> makeWrapperSetterList(List<String> columnNameList) {
//        LOG.info("columnTypeMap"+columnTypeMap.toString()+"###columnNameList:"+columnNameList.toString());
        return columnNameList
                .stream()
                .map(columnName -> getWrapperSetter(columnTypeMap.get(columnName)))
                .collect(Collectors.toList());
    }

    public HerculesRecordWriter(TaskAttemptContext context) {
        HerculesWritable.setTargetOneLevel(isColumnNameOneLevel());

        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        columnNameList = Arrays.asList(options.getTargetOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        columnTypeMap = SchemaUtils.convert(options.getTargetOptions().getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));

        wrapperSetterList = makeWrapperSetterList(columnNameList);

        emptyColumnNameList = columnNameList.size() == 0;

        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            rateLimiter = RateLimiter.create(options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null));
        }
    }

    protected WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return getIntegerSetter();
            case DOUBLE:
                return getDoubleSetter();
            case BOOLEAN:
                return getBooleanSetter();
            case STRING:
                return getStringSetter();
            case DATE:
                return getDateSetter();
            case BYTES:
                return getBytesSetter();
            case NULL:
                return getNullSetter();
            default:
                throw new MapReduceException("Unknown data type: " + dataType.name());
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
    public void write(NullWritable key, HerculesWritable value) throws IOException, InterruptedException {
        if (rateLimiter != null) {
            rateLimiter.acquire();
        }
        if (emptyColumnNameList) {
            innerMapWrite(value);
        } else {
            innerColumnWrite(value);
        }
    }

    abstract protected WrapperSetter<T> getIntegerSetter();

    abstract protected WrapperSetter<T> getDoubleSetter();

    abstract protected WrapperSetter<T> getBooleanSetter();

    abstract protected WrapperSetter<T> getStringSetter();

    abstract protected WrapperSetter<T> getDateSetter();

    abstract protected WrapperSetter<T> getBytesSetter();

    abstract protected WrapperSetter<T> getNullSetter();

    abstract protected boolean isColumnNameOneLevel();

}
