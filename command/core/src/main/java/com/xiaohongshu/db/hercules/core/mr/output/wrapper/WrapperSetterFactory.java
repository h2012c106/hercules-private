package com.xiaohongshu.db.hercules.core.mr.output.wrapper;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.ReflectUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 将数据转换策略与IO逻辑解耦
 */
public abstract class WrapperSetterFactory<T> implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(WrapperSetterFactory.class);

    protected Map<DataType, WrapperSetter<T>> wrapperSetterMap;

    @SchemaInfo
    private Schema schema;

    public WrapperSetterFactory() {
        initializeWrapperSetterMap();
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.TARGET;
    }

    private void setWrapperSetter(Map<DataType, WrapperSetter<T>> wrapperSetterMap,
                                  BaseDataType baseDataType, Function<Void, WrapperSetter<T>> setFunction) {
        try {
            WrapperSetter<T> tmpWrapper = setFunction.apply(null);
            if (tmpWrapper != null) {
                wrapperSetterMap.put(baseDataType, tmpWrapper);
            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            LOG.warn(String.format("Undefined output convert strategy of %s, exception: %s",
                    baseDataType.toString(),
                    e.getMessage()));
        }
    }

    private void initializeWrapperSetterMap() {
        wrapperSetterMap = new ConcurrentHashMap<>(BaseDataType.values().length);
        final WrapperSetterFactory self = this;
        for (BaseDataType baseDataType : BaseDataType.values()) {
            setWrapperSetter(wrapperSetterMap, baseDataType, new Function<Void, WrapperSetter<T>>() {
                @Override
                @SneakyThrows
                @SuppressWarnings("unchecked")
                public WrapperSetter<T> apply(Void aVoid) {
                    String dataTypeCapitalName = StringUtils.capitalize(baseDataType.name().toLowerCase());
                    String methodName = "get" + dataTypeCapitalName + "Setter";
                    Method getMethod = ReflectUtils.getMethod(self.getClass(), methodName);
                    try {
                        getMethod.setAccessible(true);
                        return (WrapperSetter<T>) getMethod.invoke(self);
                    } finally {
                        getMethod.setAccessible(false);
                    }
                }
            });
        }
    }

    public final boolean contains(@NonNull DataType dataType) {
        return wrapperSetterMap.containsKey(dataType);
    }

    public final WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        WrapperSetter<T> res;
        if (dataType.isCustom()) {
            final CustomDataType<?, T> customDataType = (CustomDataType<?, T>) dataType;
            res = wrapperSetterMap.computeIfAbsent(customDataType, key -> new WrapperSetter<T>() {
                @Override
                protected void setNull(T row, String rowName, String columnName, int columnSeq) throws Exception {
                    customDataType.writeNull(row, rowName, columnName, columnSeq);
                }

                @Override
                protected void setNonnull(@NonNull BaseWrapper<?> wrapper, T row, String rowName, String columnName, int columnSeq) throws Exception {
                    customDataType.write(wrapper, row, rowName, columnName, columnSeq);
                }
            });
        } else {
            res = wrapperSetterMap.get(dataType);
            if (res == null) {
                throw new MapReduceException("Unknown data type: " + dataType.toString());
            }
        }
        return res;
    }

    public static final int MAP_WRITE_COLUMN_SEQ = -1;

    public T writeMapWrapper(MapWrapper mapWrapper, T out, String columnPath) throws Exception {
        for (Map.Entry<String, BaseWrapper<?>> entry : mapWrapper.entrySet()) {
            String columnName = entry.getKey();
            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
            BaseWrapper<?> subWrapper = entry.getValue();
            DataType columnType = schema.getColumnTypeMap().getOrDefault(fullColumnName, subWrapper.getType());
            // 这里columnSeq必不用关心，因为是塞map的，map的key何谈下标
            getWrapperSetter(columnType).set(subWrapper, out, columnPath, columnName, MAP_WRITE_COLUMN_SEQ);
        }
        return out;
    }

    abstract protected BaseTypeWrapperSetter.ByteSetter<T> getByteSetter();

    abstract protected BaseTypeWrapperSetter.ShortSetter<T> getShortSetter();

    abstract protected BaseTypeWrapperSetter.IntegerSetter<T> getIntegerSetter();

    abstract protected BaseTypeWrapperSetter.LongSetter<T> getLongSetter();

    abstract protected BaseTypeWrapperSetter.LonglongSetter<T> getLonglongSetter();

    abstract protected BaseTypeWrapperSetter.FloatSetter<T> getFloatSetter();

    abstract protected BaseTypeWrapperSetter.DoubleSetter<T> getDoubleSetter();

    abstract protected BaseTypeWrapperSetter.DecimalSetter<T> getDecimalSetter();

    abstract protected BaseTypeWrapperSetter.BooleanSetter<T> getBooleanSetter();

    abstract protected BaseTypeWrapperSetter.StringSetter<T> getStringSetter();

    abstract protected BaseTypeWrapperSetter.DateSetter<T> getDateSetter();

    abstract protected BaseTypeWrapperSetter.TimeSetter<T> getTimeSetter();

    abstract protected BaseTypeWrapperSetter.DatetimeSetter<T> getDatetimeSetter();

    abstract protected BaseTypeWrapperSetter.BytesSetter<T> getBytesSetter();

    abstract protected BaseTypeWrapperSetter.NullSetter<T> getNullSetter();

    protected WrapperSetter<T> getListSetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperSetter<T> getMapSetter() {
        throw new UnsupportedOperationException();
    }

}
