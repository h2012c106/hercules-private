package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.xiaohongshu.db.hercules.core.exception.SerializeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class BooleanWrapper extends BaseWrapper<Boolean> {

    private final static DataType DATA_TYPE = DataType.BOOLEAN;

    public BooleanWrapper(Long value) {
        this(value != 0);
    }

    public BooleanWrapper(String value) {
        this(Boolean.parseBoolean(value));
    }

    public BooleanWrapper(Boolean value) {
        super(value, DATA_TYPE, 1);
    }

    @Override
    public Long asLong() {
        return getValue() ? 1L : 0L;
    }

    @Override
    public Double asDouble() {
        return getValue() ? 1d : 0d;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return getValue() ? BigDecimal.ONE : BigDecimal.ZERO;
    }

    @Override
    public BigInteger asBigInteger() {
        return getValue() ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public Boolean asBoolean() {
        return getValue();
    }

    @Override
    public Date asDate() {
        throw new SerializeException("Unsupported to convert boolean to date.");
    }

    @Override
    public String asString() {
        return getValue().toString();
    }

    @Override
    public byte[] asBytes() {
        return getValue() ? new byte[]{0x01} : new byte[]{0x00};
    }
}
