package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import lombok.NonNull;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @param <T> 数据源读入时用于表示一行的数据结构，详情可见{@link WrapperGetter}
 */
public abstract class HerculesRecordReader<T, C extends DataTypeConverter>
        extends RecordReader<NullWritable, HerculesWritable> {

    private static final Log LOG = LogFactory.getLog(HerculesRecordReader.class);

    private Map<DataType, WrapperGetter<T>> wrapperGetterMap;

    protected WrappingOptions options;

    protected List<String> columnNameList;
    protected Map<String, DataType> columnTypeMap;

    protected C converter;

    protected boolean emptyColumnNameList;

    public HerculesRecordReader(C converter) {
        this.converter = converter;
        initializeWrapperGetterMap();
    }

    abstract protected void myInitialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException;

    private void setWrapperGetter(Map<DataType, WrapperGetter<T>> wrapperGetterMap,
                                  DataType dataType, Function<Void, WrapperGetter<T>> getFunction) {
        try {
            WrapperGetter<T> tmpWrapper = getFunction.apply(null);
            if (tmpWrapper != null) {
                wrapperGetterMap.put(dataType, tmpWrapper);
            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            LOG.warn(String.format("Undefined convert strategy of %s, exception: %s",
                    dataType.toString(),
                    ExceptionUtils.getStackTrace(e)));
        }
    }

    private void initializeWrapperGetterMap() {
        wrapperGetterMap = new HashMap<>(DataType.values().length);
        for (DataType dataType : DataType.values()) {
            switch (dataType) {
                case INTEGER:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getIntegerGetter();
                        }
                    });
                    break;
                case DOUBLE:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getDoubleGetter();
                        }
                    });
                    break;
                case BOOLEAN:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getBooleanGetter();
                        }
                    });
                    break;
                case STRING:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getStringGetter();
                        }
                    });
                    break;
                case DATE:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getDateGetter();
                        }
                    });
                    break;
                case BYTES:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getBytesGetter();
                        }
                    });
                    break;
                case NULL:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getNullGetter();
                        }
                    });
                    break;
                case LIST:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getListGetter();
                        }
                    });
                    break;
                case MAP:
                    setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                        @Override
                        public WrapperGetter<T> apply(Void aVoid) {
                            return getMapGetter();
                        }
                    });
                    break;
                default:
                    throw new MapReduceException("Unknown data type: " + dataType.name());
            }
        }
    }

    @Override
    public final void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        options = new WrappingOptions();
        options.fromConfiguration(context.getConfiguration());

        // 虽然negotiator中会强制塞，但是空值似乎传不过来
        columnNameList = Arrays.asList(options.getSourceOptions().getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
        columnTypeMap = SchemaUtils.convert(options.getSourceOptions().getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));

        emptyColumnNameList = columnNameList.size()==0;

        myInitialize(split, context);
    }

    protected final WrapperGetter<T> getWrapperGetter(@NonNull DataType dataType) {
        WrapperGetter<T> res = wrapperGetterMap.get(dataType);
        if (res == null) {
            throw new MapReduceException("Unknown data type: " + dataType.name());
        } else {
            return res;
        }
    }

    abstract protected WrapperGetter<T> getIntegerGetter();

    abstract protected WrapperGetter<T> getDoubleGetter();

    abstract protected WrapperGetter<T> getBooleanGetter();

    abstract protected WrapperGetter<T> getStringGetter();

    abstract protected WrapperGetter<T> getDateGetter();

    abstract protected WrapperGetter<T> getBytesGetter();

    abstract protected WrapperGetter<T> getNullGetter();

    protected WrapperGetter<T> getListGetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperGetter<T> getMapGetter() {
        throw new UnsupportedOperationException();
    }

}
