package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import org.apache.commons.lang3.StringUtils;

public enum HBaseDataType {
    NULL,
    SHORT,
    INT,
    LONG,
    BIGDECIMAL,
    FLOAT,
    DOUBLE,
    STRING,
    BOOLEAN,
    BYTES;

    public static HBaseDataType valueOfIgnoreCase(String value) {
        for (HBaseDataType dataType : HBaseDataType.values()) {
            if (StringUtils.equalsIgnoreCase(dataType.name(), value)) {
                return dataType;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }
}
