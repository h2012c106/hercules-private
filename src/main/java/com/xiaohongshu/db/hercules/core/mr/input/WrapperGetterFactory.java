package com.xiaohongshu.db.hercules.core.mr.input;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.ReflectionUtils;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
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
                                  DataType dataType, Function<Void, WrapperGetter<T>> getFunction) {
        try {
            WrapperGetter<T> tmpWrapper = getFunction.apply(null);
            if (tmpWrapper != null) {
                wrapperGetterMap.put(dataType, tmpWrapper);
            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            LOG.warn(String.format("Undefined input convert strategy of %s, exception: %s", dataType.toString(), e.getMessage()));
        }
    }

    private void initializeWrapperGetterMap() {
        wrapperGetterMap = new HashMap<>(DataType.values().length);
        final WrapperGetterFactory self = this;
        for (DataType dataType : DataType.values()) {
            setWrapperGetter(wrapperGetterMap, dataType, new Function<Void, WrapperGetter<T>>() {
                @Override
                @SneakyThrows
                @SuppressWarnings("unchecked")
                public WrapperGetter<T> apply(Void aVoid) {
                    String dataTypeCapitalName = StringUtils.capitalize(dataType.name().toLowerCase());
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

    public final WrapperGetter<T> getWrapperGetter(@NonNull DataType dataType) {
        WrapperGetter<T> res = wrapperGetterMap.get(dataType);
        if (res == null) {
            throw new MapReduceException("Unsupported data type: " + dataType.name());
        } else {
            return res;
        }
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
