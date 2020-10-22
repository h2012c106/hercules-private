package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NullWrapper extends BaseWrapper<String> {

    public final static NullWrapper INSTANCE = new NullWrapper();

    private NullWrapper(DataType dataType) {
        super("", dataType, 0);
    }

    private NullWrapper() {
        this(BaseDataType.NULL);
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
    public InfinitableBigDecimal asBigDecimal() {
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
    public ExtendedDate asDate() {
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

    @Override
    public int compareTo(String o) {
        return 0;
    }
}
