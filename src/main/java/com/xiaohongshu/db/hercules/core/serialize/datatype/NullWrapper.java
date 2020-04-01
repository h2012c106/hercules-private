package com.xiaohongshu.db.hercules.core.serialize.datatype;

import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class NullWrapper extends BaseWrapper<String> {

    private final static DataType DATA_TYPE = DataType.NULL;

    public final static NullWrapper INSTANCE = new NullWrapper();

    public NullWrapper() {
        super("", DATA_TYPE, 0);
    }

    @Override
    public Long asLong() {
        return null;
    }

    @Override
    public Double asDouble() {
        return null;
    }

    @Override
    public BigDecimal asBigDecimal() {
        return null;
    }

    @Override
    public BigInteger asBigInteger() {
        return null;
    }

    @Override
    public Boolean asBoolean() {
        return null;
    }

    @Override
    public Date asDate() {
        return null;
    }

    @Override
    public String asString() {
        return null;
    }

    @Override
    public byte[] asBytes() {
        return null;
    }
}
