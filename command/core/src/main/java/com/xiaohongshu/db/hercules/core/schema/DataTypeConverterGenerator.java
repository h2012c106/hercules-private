package com.xiaohongshu.db.hercules.core.schema;

public interface DataTypeConverterGenerator<C extends DataTypeConverter<?, ?>> {
    C generateConverter();
}
