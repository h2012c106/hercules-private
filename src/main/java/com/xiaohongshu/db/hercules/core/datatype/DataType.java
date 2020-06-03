package com.xiaohongshu.db.hercules.core.datatype;

public interface DataType {
    public Class<?> getStorageClass();

    public BaseDataType getBaseDataType();

    public boolean isCustom();

    public static DataType valueOfIgnoreCase(String typeName, BaseCustomDataTypeManager manager) {
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
