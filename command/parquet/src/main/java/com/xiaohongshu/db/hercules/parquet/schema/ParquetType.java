package com.xiaohongshu.db.hercules.parquet.schema;

import org.apache.parquet.schema.Type;

/**
 * 包含是否无视repeated的信息
 */
public class ParquetType {
    private Type type;
    private boolean careRepeated;

    public ParquetType(Type type) {
        this(type, true);
    }

    public ParquetType(Type type, boolean careRepeated) {
        this.type = type;
        this.careRepeated = careRepeated;
    }

    public Type getType() {
        return type;
    }

    public boolean careRepeated() {
        return careRepeated;
    }
}
