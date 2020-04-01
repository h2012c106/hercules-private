package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.xiaohongshu.db.hercules.core.utils.command.OverflowUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 整数
 *
 * @author huanghanxiang
 */
public class IntegerWrapper extends BaseWrapper<BigInteger> {

    private final static DataType DATA_TYPE = DataType.INTEGER;

    public IntegerWrapper(Integer value) {
        this(BigInteger.valueOf(value), 4);
    }

    public IntegerWrapper(Long value) {
        this(BigInteger.valueOf(value), 8);
    }

    public IntegerWrapper(String value) {
        this(NumberUtils.createBigInteger(value), value.length());
    }

    public IntegerWrapper(BigInteger value) {
        // 其实应该根据内部mag数组的大小来算，偷懒
        this(value, value.toString().length());
    }

    private IntegerWrapper(BigInteger value, int byteSize) {
        super(value, DATA_TYPE, byteSize);
    }

    @Override
    public Long asLong() {
        OverflowUtils.validateLong(getValue());
        return getValue().longValue();
    }

    @Override
    public Double asDouble() {
        BigDecimal value = asBigDecimal();
        OverflowUtils.validateDouble(value);
        return value.doubleValue();
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
}
