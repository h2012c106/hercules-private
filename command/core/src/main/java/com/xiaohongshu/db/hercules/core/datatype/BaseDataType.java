package com.xiaohongshu.db.hercules.core.datatype;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public enum BaseDataType implements DataType {
    NULL(
            String.class,
            Void.class,
            obj -> NullWrapper.INSTANCE,
            wrapper -> null
    ),
    BYTE(
            BigInteger.class,
            Byte.class,
            obj -> IntegerWrapper.get((Byte) obj),
            wrapper -> wrapper.asBigInteger().byteValueExact()
    ),
    SHORT(
            BigInteger.class,
            Short.class,
            obj -> IntegerWrapper.get((Short) obj),
            wrapper -> wrapper.asBigInteger().shortValueExact()
    ),
    INTEGER(
            BigInteger.class,
            Integer.class,
            obj -> IntegerWrapper.get((Integer) obj),
            wrapper -> wrapper.asBigInteger().intValueExact()
    ),
    LONG(
            BigInteger.class,
            Long.class,
            obj -> IntegerWrapper.get((Long) obj),
            wrapper -> wrapper.asBigInteger().longValueExact()
    ),
    LONGLONG(
            BigInteger.class,
            BigInteger.class,
            obj -> IntegerWrapper.get((BigInteger) obj),
            BaseWrapper::asBigInteger
    ),
    BOOLEAN(
            Boolean.class,
            Boolean.class,
            obj -> BooleanWrapper.get((Boolean) obj),
            BaseWrapper::asBoolean
    ),
    FLOAT(
            InfinitableBigDecimal.class,
            Float.class,
            obj -> DoubleWrapper.get((Float) obj),
            wrapper -> wrapper.asBigDecimal().getFloatValue()
    ),
    DOUBLE(
            InfinitableBigDecimal.class,
            Double.class,
            obj -> DoubleWrapper.get((Double) obj),
            wrapper -> wrapper.asBigDecimal().getDoubleValue()
    ),
    DECIMAL(
            InfinitableBigDecimal.class,
            BigDecimal.class,
            obj -> DoubleWrapper.get((BigDecimal) obj),
            wrapper -> wrapper.asBigDecimal().getDecimalValue()
    ),
    STRING(
            String.class,
            String.class,
            obj -> StringWrapper.get((String) obj),
            BaseWrapper::asString
    ),
    DATE(
            ExtendedDate.class,
            ExtendedDate.class,
            obj -> DateWrapper.getDate((ExtendedDate) obj),
            BaseWrapper::asDate
    ),
    TIME(
            ExtendedDate.class,
            ExtendedDate.class,
            obj -> DateWrapper.getTime((ExtendedDate) obj),
            BaseWrapper::asDate
    ),
    DATETIME(
            ExtendedDate.class,
            ExtendedDate.class,
            obj -> DateWrapper.getDatetime((ExtendedDate) obj),
            BaseWrapper::asDate
    ),
    BYTES(
            byte[].class,
            byte[].class,
            obj -> BytesWrapper.get((byte[]) obj),
            BaseWrapper::asBytes
    ),
    LIST(
            List.class,
            List.class,
            obj -> {
                throw new UnsupportedOperationException();
            },
            wrapper -> {
                throw new UnsupportedOperationException();
            }
    ),
    MAP(
            Map.class,
            Map.class,
            obj -> {
                throw new UnsupportedOperationException();
            },
            wrapper -> {
                throw new UnsupportedOperationException();
            }
    );

    public static BaseDataType valueOfIgnoreCase(String value) {
        for (BaseDataType baseDataType : BaseDataType.values()) {
            if (StringUtils.equalsIgnoreCase(baseDataType.name(), value)) {
                return baseDataType;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }

    private Class<?> storageClass;

    private Class<?> javaClass;

    private Function<Object, BaseWrapper<?>> readFunction;

    private Function<BaseWrapper<?>, Object> writeFunction;

    BaseDataType(Class<?> storageClass, Class<?> javaClass, Function<Object, BaseWrapper<?>> readFunction, Function<BaseWrapper<?>, Object> writeFunction) {
        this.storageClass = storageClass;
        this.javaClass = javaClass;
        this.readFunction = readFunction;
        this.writeFunction = writeFunction;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public Class<?> getJavaClass() {
        return javaClass;
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

    @Override
    public Function<Object, BaseWrapper<?>> getReadFunction() {
        return readFunction;
    }

    @Override
    public Function<BaseWrapper<?>, Object> getWriteFunction() {
        return writeFunction;
    }

    public boolean isInteger() {
        switch (this) {
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case LONGLONG:
                return true;
            default:
                return false;
        }
    }

    public boolean isFloat() {
        switch (this) {
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    public boolean isNumber() {
        return isInteger() || isFloat();
    }

    public boolean isBoolean() {
        return this == BOOLEAN;
    }

    public boolean isBytes() {
        return this == BYTES;
    }

    public boolean isString() {
        return this == STRING;
    }

    public boolean isDate() {
        switch (this) {
            case DATE:
            case TIME:
            case DATETIME:
                return true;
            default:
                return false;
        }
    }

    public boolean isNested() {
        return this == LIST || this == MAP;
    }

    public boolean isNull() {
        return this == NULL;
    }
}
