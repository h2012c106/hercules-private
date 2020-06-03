package com.xiaohongshu.db.hercules.core.mr.output;

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
 */
public abstract class WrapperSetterFactory<T> {

    private static final Log LOG = LogFactory.getLog(WrapperSetterFactory.class);

    private Map<DataType, WrapperSetter<T>> wrapperSetterMap;

    public WrapperSetterFactory() {
        initializeWrapperSetterMap();
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

    public final boolean contains(@NonNull DataType dataType) {
        return wrapperSetterMap.containsKey(dataType);
    }

    public final WrapperSetter<T> getWrapperSetter(@NonNull DataType dataType) {
        WrapperSetter<T> res;
        if (dataType.isCustom()) {
            final CustomDataType<?, T> customDataType = (CustomDataType<?, T>) dataType;
            res = wrapperSetterMap.computeIfAbsent(customDataType, key -> new WrapperSetter<T>() {
                @Override
                public void set(@NonNull BaseWrapper wrapper, T row, String rowName, String columnName, int columnSeq) throws Exception {
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
