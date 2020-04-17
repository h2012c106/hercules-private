package com.xiaohongshu.db.hercules.core.serialize.datatype;

public enum DataType {
    NULL,
    INTEGER,
    DOUBLE,
    STRING,
    BOOLEAN,
    DATE,
    BYTES,
    /**
     * 应该仅在出数据时用到这个enum，进时尽可能使用{@link #LIST}和{@link #MAP}
     */
    JSON,
    LIST,
    MAP;
}
