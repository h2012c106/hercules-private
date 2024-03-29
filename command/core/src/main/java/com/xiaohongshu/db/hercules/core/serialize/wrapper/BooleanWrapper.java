package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BooleanWrapper extends BaseWrapper<Boolean> {

    private final static DataType DATA_TYPE = BaseDataType.BOOLEAN;

    private final static BooleanWrapper TRUE_BOOLEAN_WRAPPER = new BooleanWrapper(true);
    private final static BooleanWrapper FALSE_BOOLEAN_WRAPPER = new BooleanWrapper(false);

    protected BooleanWrapper(Boolean value) {
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
    public InfinitableBigDecimal asBigDecimal() {
        return getValue() ? InfinitableBigDecimal.ONE : InfinitableBigDecimal.ZERO;
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
    public int compareTo(Boolean o) {
        return getValue().compareTo(o);
    }
}
