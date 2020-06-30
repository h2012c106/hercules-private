package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NullWrapper extends BaseWrapper<String> {

    private final static DataType DATA_TYPE = BaseDataType.NULL;

    public final static NullWrapper INSTANCE = new NullWrapper();

    private NullWrapper(DataType dataType) {
        super("", DATA_TYPE, 0);
    }

    private NullWrapper() {
        this(DATA_TYPE);
    }

    private static final Map<DataType, NullWrapper> FACTORY = new ConcurrentHashMap<>();

    public static NullWrapper get(DataType dataType) {
        return FACTORY.computeIfAbsent(dataType, NullWrapper::new);
    }

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public Byte asByte() {
        return null;
    }

    @Override
    public Short asShort() {
        return null;
    }

    @Override
    public Integer asInteger() {
        return null;
    }

    @Override
    public Long asLong() {
        return null;
    }

    @Override
    public Float asFloat() {
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

    @Override
    public JSON asJson() {
        return null;
    }

    @Override
    public Object asDefault() {
        return null;
    }
}
