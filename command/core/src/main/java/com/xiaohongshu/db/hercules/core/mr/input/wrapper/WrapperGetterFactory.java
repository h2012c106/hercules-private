package com.xiaohongshu.db.hercules.core.mr.input.wrapper;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.utils.reflect.ReflectUtils;
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
 *
 * @param <T>
 */
public abstract class WrapperGetterFactory<T> implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(WrapperGetterFactory.class);

    private Map<DataType, WrapperGetter<T>> wrapperGetterMap;

    public WrapperGetterFactory() {
        initializeWrapperGetterMap();
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SOURCE;
    }

    private void setWrapperGetter(Map<DataType, WrapperGetter<T>> wrapperGetterMap,
                                  BaseDataType baseDataType, Function<Void, WrapperGetter<T>> getFunction) {
        try {
            WrapperGetter<T> tmpWrapper = getFunction.apply(null);
            if (tmpWrapper != null) {
                wrapperGetterMap.put(baseDataType, tmpWrapper);
            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            LOG.warn(String.format("Undefined input convert strategy of %s, exception: %s", baseDataType.toString(), e.getMessage()));
        }
    }

    private void initializeWrapperGetterMap() {
        wrapperGetterMap = new ConcurrentHashMap<>(BaseDataType.values().length);
        final WrapperGetterFactory self = this;
        for (BaseDataType baseDataType : BaseDataType.values()) {
            setWrapperGetter(wrapperGetterMap, baseDataType, new Function<Void, WrapperGetter<T>>() {
                @Override
                @SneakyThrows
                @SuppressWarnings("unchecked")
                public WrapperGetter<T> apply(Void aVoid) {
                    String dataTypeCapitalName = StringUtils.capitalize(baseDataType.name().toLowerCase());
                    String methodName = "get" + dataTypeCapitalName + "Getter";
                    Method getMethod = ReflectUtils.getMethod(self.getClass(), methodName);
                    try {
                        getMethod.setAccessible(true);
                        return (WrapperGetter<T>) getMethod.invoke(self);
                    } finally {
                        getMethod.setAccessible(false);
                    }
                }
            });
        }
    }

    public final boolean contains(@NonNull DataType dataType) {
        return wrapperGetterMap.containsKey(dataType);
    }

    public final WrapperGetter<T> getWrapperGetter(@NonNull DataType dataType) {
        WrapperGetter<T> res;
        if (dataType.isCustom()) {
            final CustomDataType<T, ?, ?> customDataType = (CustomDataType<T, ?, ?>) dataType;
            // 运行时赋
            res = wrapperGetterMap.computeIfAbsent(customDataType, key -> customDataType.getWrapperGetter());
        } else {
            res = wrapperGetterMap.get(dataType);
            if (res == null) {
                throw new MapReduceException("Unsupported data type: " + dataType.toString());
            }
        }
        return res;
    }

    abstract protected BaseTypeWrapperGetter.ByteGetter<T> getByteGetter();

    abstract protected BaseTypeWrapperGetter.ShortGetter<T> getShortGetter();

    abstract protected BaseTypeWrapperGetter.IntegerGetter<T> getIntegerGetter();

    abstract protected BaseTypeWrapperGetter.LongGetter<T> getLongGetter();

    abstract protected BaseTypeWrapperGetter.LonglongGetter<T> getLonglongGetter();

    abstract protected BaseTypeWrapperGetter.FloatGetter<T> getFloatGetter();

    abstract protected BaseTypeWrapperGetter.DoubleGetter<T> getDoubleGetter();

    abstract protected BaseTypeWrapperGetter.DecimalGetter<T> getDecimalGetter();

    abstract protected BaseTypeWrapperGetter.BooleanGetter<T> getBooleanGetter();

    abstract protected BaseTypeWrapperGetter.StringGetter<T> getStringGetter();

    abstract protected BaseTypeWrapperGetter.DateGetter<T> getDateGetter();

    abstract protected BaseTypeWrapperGetter.TimeGetter<T> getTimeGetter();

    abstract protected BaseTypeWrapperGetter.DatetimeGetter<T> getDatetimeGetter();

    abstract protected BaseTypeWrapperGetter.BytesGetter<T> getBytesGetter();

    abstract protected BaseTypeWrapperGetter.NullGetter<T> getNullGetter();

    protected WrapperGetter<T> getListGetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperGetter<T> getMapGetter() {
        throw new UnsupportedOperationException();
    }
}
