package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class DoubleWrapper extends BaseWrapper<BigDecimal> {

    public DoubleWrapper(Float value) {
        this(BigDecimal.valueOf(value), DataType.FLOAT, 4);
    }

    public DoubleWrapper(Double value) {
        this(BigDecimal.valueOf(value), DataType.DOUBLE, 8);
    }

    public DoubleWrapper(BigDecimal value) {
        // 仅仅粗略估计
        this(value, DataType.DECIMAL, value.toBigInteger().toByteArray().length);
    }

    private DoubleWrapper(BigDecimal value, DataType type, int byteSize) {
        super(value, type, byteSize);
    }

    @Override
    public Long asLong() {
        return getValue().longValueExact();
    }

    @Override
    public Double asDouble() {
        return OverflowUtils.numberToDouble(getValue());
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
