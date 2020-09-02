package com.xiaohongshu.db.hercules.core.datatype;

import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import lombok.NonNull;

import java.util.function.Function;

public interface DataType {

    public String getName();

    /**
     * 实际在java中的类型，可能没啥用
     * @return
     */
    public Class<?> getJavaClass();

    public Class<?> getStorageClass();

    public BaseDataType getBaseDataType();

    public boolean isCustom();

    public Function<Object, BaseWrapper<?>> getReadFunction();

    public Function<BaseWrapper<?>, Object> getWriteFunction();

    public static DataType valueOfIgnoreCase(String typeName, @NonNull CustomDataTypeManager<?, ?> manager) {
        // 先看基本类型（同名以基本类型为准）
        try {
            return BaseDataType.valueOfIgnoreCase(typeName);
        } catch (Exception ignore) {
        }
        try {
            return manager.getIgnoreCase(typeName);
        } catch (Exception ignore) {
        }
        throw new RuntimeException(String.format("Cannot find the [%s] type neither in BaseDataType nor in %s.",
                typeName, manager.getClass().getCanonicalName()));
    }
}
