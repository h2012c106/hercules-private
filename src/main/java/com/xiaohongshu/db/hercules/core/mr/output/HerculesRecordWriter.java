package com.xiaohongshu.db.hercules.core.mr.output;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.RateLimiter;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.BaseSchemaFetcher;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @param <T> 数据源写出时用于表示一行的数据结构，详情可见{@link WrapperSetter}
 * @param <S>
 */
public abstract class HerculesRecordWriter<T, S extends BaseSchemaFetcher> extends RecordWriter<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordWriter.class);

    protected WrappingOptions options;
    protected List<WrapperSetter<T>> wrapperSetterList;

    /**
     * 上游没有的列，这里会置null
     */
    protected String[] columnNames;

    protected S schemaFetcher;

    protected List<String> sourceColumnList;

    protected RateLimiter rateLimiter = null;

    protected <X> List<WrapperSetter<T>> makeWrapperSetterList(final BaseSchemaFetcher<X> schemaFetcher, List<String> columnNameList) {
        return columnNameList
                .stream()
                .map(columnName -> getWrapperSetter(schemaFetcher.getColumnTypeMap().get(columnName)))
                .collect(Collectors.toList());
    }

    public HerculesRecordWriter(TaskAttemptContext context, S schemaFetcher) {
        HerculesWritable.setTargetOneLevel(isColumnNameOneLevel());

        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        this.schemaFetcher = schemaFetcher;

        columnNames = options.getTargetOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null);

        wrapperSetterList = makeWrapperSetterList(schemaFetcher, Arrays.asList(columnNames));

        if (options.getCommonOptions().hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            rateLimiter = RateLimiter.create(options.getCommonOptions().getDouble(CommonOptionsConf.MAX_WRITE_QPS, null));
        }
    }

    private WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
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

    abstract protected void innerWrite(NullWritable key, HerculesWritable value) throws IOException, InterruptedException;

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
        innerWrite(key, value);
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
