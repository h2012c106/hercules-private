package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

public enum DataType {
    NULL,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    LONGLONG,
    BOOLEAN,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    DATE,
    TIME,
    DATETIME,
    BYTES,
    LIST,
    MAP;

    public static DataType valueOfIgnoreCase(String value) {
        for (DataType dataType : DataType.values()) {
            if (StringUtils.equalsIgnoreCase(dataType.name(), value)) {
                return dataType;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }
}
