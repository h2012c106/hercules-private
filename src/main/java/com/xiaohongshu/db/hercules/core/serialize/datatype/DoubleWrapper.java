package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class DoubleWrapper extends BaseWrapper<BigDecimal> {

    private final static DataType DATA_TYPE = DataType.DOUBLE;

    public DoubleWrapper(Float value) {
        this(BigDecimal.valueOf(value), 4);
    }

    public DoubleWrapper(Double value) {
        this(BigDecimal.valueOf(value), 8);
    }

    public DoubleWrapper(String value) {
        this(NumberUtils.createBigDecimal(value), value.length());
    }

    public DoubleWrapper(BigDecimal value) {
        this(value, value.toPlainString().length());
    }

    private DoubleWrapper(BigDecimal value, int byteSize) {
        super(value, DATA_TYPE, byteSize);
    }

    @Override
    public Long asLong() {
        OverflowUtils.validateLong(getValue().toBigInteger());
        return getValue().longValue();
    }

    @Override
    public Double asDouble() {
        OverflowUtils.validateDouble(getValue());
        return getValue().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal() {
        return getValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return getValue().toBigInteger();
    }

    @Override
    public Boolean asBoolean() {
        return getValue().compareTo(BigDecimal.ZERO) != 0;
    }

    @Override
    public Date asDate() {
        return new Date(asLong());
    }

    @Override
    public String asString() {
        return getValue().toPlainString();
    }

    @Override
    public byte[] asBytes() {
        return asBigInteger().toByteArray();
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert number to json.");
    }
}
