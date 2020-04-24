package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.stream.Collectors;

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

    public static DataType valueOfIgnoreCase(String value) {
        for (DataType dataType : DataType.values()) {
            if (StringUtils.equalsIgnoreCase(dataType.name(), value)) {
                return dataType;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }
}
