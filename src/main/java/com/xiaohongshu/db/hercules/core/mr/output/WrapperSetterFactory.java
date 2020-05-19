package com.xiaohongshu.db.hercules.core.mr.output;

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
 */
public abstract class WrapperSetterFactory<T> {

    private static final Log LOG = LogFactory.getLog(WrapperSetterFactory.class);

    private Map<DataType, WrapperSetter<T>> wrapperSetterMap;

    public WrapperSetterFactory() {
        initializeWrapperSetterMap();
    }

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
            LOG.warn(String.format("Undefined output convert strategy of %s, exception: %s",
                    dataType.toString(),
                    e.getMessage()));
        }
    }

    private void initializeWrapperSetterMap() {
        wrapperSetterMap = new HashMap<>(DataType.values().length);
        final WrapperSetterFactory self = this;
        for (DataType dataType : DataType.values()) {
            setWrapperSetter(wrapperSetterMap, dataType, new Function<Void, WrapperSetter<T>>() {
                @Override
                @SneakyThrows
                @SuppressWarnings("unchecked")
                public WrapperSetter<T> apply(Void aVoid) {
                    String dataTypeCapitalName = StringUtils.capitalize(dataType.name().toLowerCase());
                    String methodName = "get" + dataTypeCapitalName + "Setter";
                    Method getMethod = ReflectionUtils.getMethod(self.getClass(), methodName);
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

    public final WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        WrapperSetter<T> res = wrapperSetterMap.get(dataType);
        if (res == null) {
            throw new MapReduceException("Unknown data type: " + dataType.name());
        } else {
            return res;
        }
    }

    abstract protected WrapperSetter<T> getByteSetter();

    abstract protected WrapperSetter<T> getShortSetter();

    abstract protected WrapperSetter<T> getIntegerSetter();

    abstract protected WrapperSetter<T> getLongSetter();

    abstract protected WrapperSetter<T> getLonglongSetter();

    abstract protected WrapperSetter<T> getFloatSetter();

    abstract protected WrapperSetter<T> getDoubleSetter();

    abstract protected WrapperSetter<T> getDecimalSetter();

    abstract protected WrapperSetter<T> getBooleanSetter();

    abstract protected WrapperSetter<T> getStringSetter();

    abstract protected WrapperSetter<T> getDateSetter();

    abstract protected WrapperSetter<T> getTimeSetter();

    abstract protected WrapperSetter<T> getDatetimeSetter();

    abstract protected WrapperSetter<T> getBytesSetter();

    abstract protected WrapperSetter<T> getNullSetter();

    protected WrapperSetter<T> getListSetter() {
        throw new UnsupportedOperationException();
    }

    protected WrapperSetter<T> getMapSetter() {
        throw new UnsupportedOperationException();
    }

}
