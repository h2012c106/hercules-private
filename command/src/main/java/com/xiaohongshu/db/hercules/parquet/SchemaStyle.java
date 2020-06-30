package com.xiaohongshu.db.hercules.parquet;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum SchemaStyle {
    SQOOP,
    HIVE,
    ORIGINAL;

    public static SchemaStyle valueOfIgnoreCase(String value) {
        for (SchemaStyle schemaStyle : SchemaStyle.values()) {
            if (StringUtils.equalsIgnoreCase(schemaStyle.name(), value)) {
                return schemaStyle;
            }
        }
        throw new ParseException("Illegal schema style: " + value);
    }
}
