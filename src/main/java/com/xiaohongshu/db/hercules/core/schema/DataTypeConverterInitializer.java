package com.xiaohongshu.db.hercules.core.schema;

public interface DataTypeConverterInitializer<T extends DataTypeConverter> {
    T initializeConverter();
}
