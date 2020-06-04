package com.xiaohongshu.db.hercules.core.datatype;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public enum BaseDataType implements DataType {
    NULL(String.class),
    BYTE(BigInteger.class),
    SHORT(BigInteger.class),
    INTEGER(BigInteger.class),
    LONG(BigInteger.class),
    LONGLONG(BigInteger.class),
    BOOLEAN(Boolean.class),
    FLOAT(BigDecimal.class),
    DOUBLE(BigDecimal.class),
    DECIMAL(BigDecimal.class),
    STRING(String.class),
    DATE(String.class),
    TIME(String.class),
    DATETIME(String.class),
    BYTES(byte[].class),
    LIST(List.class),
    MAP(Map.class);

    public static BaseDataType valueOfIgnoreCase(String value) {
        for (BaseDataType baseDataType : BaseDataType.values()) {
            if (StringUtils.equalsIgnoreCase(baseDataType.name(), value)) {
                return baseDataType;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }

    private Class<?> storageClass;

    private BaseDataType(Class<?> storageClass) {
        this.storageClass = storageClass;
    }

    @Override
    public Class<?> getStorageClass() {
        return storageClass;
    }

    @Override
    public BaseDataType getBaseDataType() {
        return this;
    }

    @Override
    public boolean isCustom() {
        return false;
    }
}
