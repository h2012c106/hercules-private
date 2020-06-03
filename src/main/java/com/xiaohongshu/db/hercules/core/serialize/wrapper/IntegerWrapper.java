package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
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

    private IntegerWrapper(Byte value) {
        this(BigInteger.valueOf(value), BaseDataType.BYTE, 1);
    }

    private IntegerWrapper(Short value) {
        this(BigInteger.valueOf(value), BaseDataType.SHORT, 2);
    }

    private IntegerWrapper(Integer value) {
        this(BigInteger.valueOf(value), BaseDataType.INTEGER, 4);
    }

    private IntegerWrapper(Long value) {
        this(BigInteger.valueOf(value), BaseDataType.LONG, 8);
    }

    private IntegerWrapper(BigInteger value) {
        this(value, BaseDataType.LONGLONG, value.toByteArray().length);
    }

    private IntegerWrapper(BigInteger value, DataType type, int byteSize) {
        super(value, type, byteSize);
    }

    public static BaseWrapper get(Byte value) {
        return value == null ? NullWrapper.get(BaseDataType.BYTE) : new IntegerWrapper(value);
    }

    public static BaseWrapper get(Short value) {
        return value == null ? NullWrapper.get(BaseDataType.SHORT) : new IntegerWrapper(value);
    }

    public static BaseWrapper get(Integer value) {
        return value == null ? NullWrapper.get(BaseDataType.INTEGER) : new IntegerWrapper(value);
    }

    public static BaseWrapper get(Long value) {
        return value == null ? NullWrapper.get(BaseDataType.LONG) : new IntegerWrapper(value);
    }

    public static BaseWrapper get(BigInteger value) {
        return value == null ? NullWrapper.get(BaseDataType.LONGLONG) : new IntegerWrapper(value);
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
