package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 整数
 *
 * @author huanghanxiang
 */
public class IntegerWrapper extends BaseWrapper<BigInteger> {

    public IntegerWrapper(Byte value) {
        this(BigInteger.valueOf(value), DataType.BYTE, 1);
    }

    public IntegerWrapper(Short value) {
        this(BigInteger.valueOf(value), DataType.SHORT, 2);
    }

    public IntegerWrapper(Integer value) {
        this(BigInteger.valueOf(value), DataType.INTEGER, 4);
    }

    public IntegerWrapper(Long value) {
        this(BigInteger.valueOf(value), DataType.LONG, 8);
    }

    public IntegerWrapper(BigInteger value) {
        this(value, DataType.LONGLONG, value.toByteArray().length);
    }

    private IntegerWrapper(BigInteger value, DataType type, int byteSize) {
        super(value, type, byteSize);
    }

    @Override
    public Long asLong() {
        return getValue().longValueExact();
    }

    @Override
    public Double asDouble() {
        BigDecimal value = asBigDecimal();
        return OverflowUtils.numberToDouble(value);
    }

    @Override
    public BigDecimal asBigDecimal() {
        return new BigDecimal(getValue());
    }

    @Override
    public BigInteger asBigInteger() {
        return getValue();
    }

    @Override
    public Boolean asBoolean() {
        return getValue().compareTo(BigInteger.ZERO) != 0;
    }

    @Override
    public Date asDate() {
        return new Date(asLong());
    }

    @Override
    public String asString() {
        return getValue().toString();
    }

    @Override
    public byte[] asBytes() {
        return getValue().toByteArray();
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert number to json.");
    }
}
