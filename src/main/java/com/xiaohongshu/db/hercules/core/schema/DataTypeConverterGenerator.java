package com.xiaohongshu.db.hercules.core.schema;

public interface DataTypeConverterGenerator<T extends DataTypeConverter> {
    T generateConverter();
}
