package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BooleanWrapper extends BaseWrapper<Boolean> {

    private final static DataType DATA_TYPE = BaseDataType.BOOLEAN;

    private final static BooleanWrapper TRUE_BOOLEAN_WRAPPER = new BooleanWrapper(true);
    private final static BooleanWrapper FALSE_BOOLEAN_WRAPPER = new BooleanWrapper(false);

    private BooleanWrapper(Boolean value) {
        super(value, DATA_TYPE, 1);
    }

    public static BaseWrapper<?> get(Boolean value) {
        if (value == null) {
            return NullWrapper.get(DATA_TYPE);
        } else {
            return value ? TRUE_BOOLEAN_WRAPPER : FALSE_BOOLEAN_WRAPPER;
        }
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
    public ExtendedDate asDate() {
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

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert boolean to json.");
    }

    @Override
    public int compareTo(BaseWrapper<?> o) {
        return getValue().compareTo(o.asBoolean());
    }
}
