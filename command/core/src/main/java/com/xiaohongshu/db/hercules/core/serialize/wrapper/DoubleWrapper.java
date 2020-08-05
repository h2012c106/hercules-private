package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class DoubleWrapper extends BaseWrapper<BigDecimal> {

    private DoubleWrapper(Float value) {
        this(BigDecimal.valueOf(value), BaseDataType.FLOAT, 4);
    }

    private DoubleWrapper(Double value) {
        this(BigDecimal.valueOf(value), BaseDataType.DOUBLE, 8);
    }

    private DoubleWrapper(BigDecimal value) {
        // 仅仅粗略估计
        this(value, BaseDataType.DECIMAL, value.toBigInteger().toByteArray().length);
    }

    private DoubleWrapper(BigDecimal value, DataType type, int byteSize) {
        super(value, type, byteSize);
    }

    public static BaseWrapper get(Float value) {
        return value == null ? NullWrapper.get(BaseDataType.FLOAT) : new DoubleWrapper(value);
    }

    public static BaseWrapper get(Double value) {
        return value == null ? NullWrapper.get(BaseDataType.DOUBLE) : new DoubleWrapper(value);
    }

    public static BaseWrapper get(BigDecimal value) {
        return value == null ? NullWrapper.get(BaseDataType.DECIMAL) : new DoubleWrapper(value);
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
    public ExtendedDate asDate() {
        return new ExtendedDate(new Date(asLong()));
    }

    @Override
    public String asString() {
        return getValue().toPlainString();
    }

    @Override
    public byte[] asBytes() {
        throw new SerializeException("Unsupported to convert number to bytes.");
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert number to json.");
    }
}
