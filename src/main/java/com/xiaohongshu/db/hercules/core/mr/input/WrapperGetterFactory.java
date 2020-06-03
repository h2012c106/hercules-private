package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.ReflectionUtils;
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
public abstract class WrapperGetterFactory<T> {

    private static final Log LOG = LogFactory.getLog(WrapperGetterFactory.class);

    private Map<DataType, WrapperGetter<T>> wrapperGetterMap;

    public WrapperGetterFactory() {
        initializeWrapperGetterMap();
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
                    Method getMethod = ReflectionUtils.getMethod(self.getClass(), methodName);
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
            final CustomDataType<T, ?> customDataType = (CustomDataType<T, ?>) dataType;
            // 运行时赋
            res = wrapperGetterMap.computeIfAbsent(customDataType, key -> new WrapperGetter<T>() {
                @Override
                public BaseWrapper get(T row, String rowName, String columnName, int columnSeq) throws Exception {
                    return customDataType.read(row, rowName, columnName, columnSeq);
                }
            });
        } else {
            res = wrapperGetterMap.get(dataType);
            if (res == null) {
                throw new MapReduceException("Unsupported data type: " + dataType.toString());
            }
        }
        return res;
    }

    abstract protected WrapperGetter<T> getByteGetter();

    abstract protected WrapperGetter<T> getShortGetter();

    abstract protected WrapperGetter<T> getIntegerGetter();

    abstract protected WrapperGetter<T> getLongGetter();

    abstract protected WrapperGetter<T> getLonglongGetter();

    abstract protected WrapperGetter<T> getFloatGetter();

    abstract protected WrapperGetter<T> getDoubleGetter();

    abstract protected WrapperGetter<T> getDecimalGetter();

    abstract protected WrapperGetter<T> getBooleanGetter();

    abstract protected WrapperGetter<T> getStringGetter();

    abstract protected WrapperGetter<T> getDateGetter();

    abstract protected WrapperGetter<T> getTimeGetter();

    abstract protected WrapperGetter<T> getDatetimeGetter();

    abstract protected WrapperGetter<T> getBytesGetter();

    abstract protected WrapperGetter<T> getNullGetter();

    protected WrapperGetter<T> getListGetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperGetter<T> getMapGetter() {
        throw new UnsupportedOperationException();
    }
}
