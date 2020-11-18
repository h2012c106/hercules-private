package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class DoubleWrapper extends BaseWrapper<InfinitableBigDecimal> {

    protected DoubleWrapper(Float value) {
        this(InfinitableBigDecimal.valueOf(value), BaseDataType.FLOAT, 4);
    }

    protected DoubleWrapper(Double value) {
        this(InfinitableBigDecimal.valueOf(value), BaseDataType.DOUBLE, 8);
    }

    protected DoubleWrapper(BigDecimal value) {
        // 仅仅粗略估计
        this(InfinitableBigDecimal.valueOf(value), BaseDataType.DECIMAL, value.toBigInteger().toByteArray().length);
    }

    protected DoubleWrapper(InfinitableBigDecimal value, DataType type, int byteSize) {
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
    public InfinitableBigDecimal asBigDecimal() {
        return getValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return asBigDecimal().getDecimalValue().toBigInteger();
    }

    @Override
    public Boolean asBoolean() {
        return asBigDecimal().getDecimalValue().compareTo(BigDecimal.ZERO) != 0;
    }

    @Override
    public ExtendedDate asDate() {
        return ExtendedDate.initialize(new Date(asBigInteger().longValueExact()));
    }

    @Override
    public String asString() {
        return getValue().toString();
    }

    @Override
    public byte[] asBytes() {
        throw new SerializeException("Unsupported to convert number to bytes.");
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert number to json.");
    }

    @Override
    public Integer compareWith(BaseWrapper<?> that) {
        // 不优雅
        if (that.getClass() == IntegerWrapper.class) {
            return asBigDecimal().compareTo(that.asBigDecimal());
        }
        return super.compareWith(that);
    }

    @Override
    public int compareTo(InfinitableBigDecimal o) {
        return getValue().compareTo(o);
    }
}
