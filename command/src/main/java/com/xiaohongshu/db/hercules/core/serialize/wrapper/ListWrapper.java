package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 没有对应的asXXX方法，因为本质上是一个wrapper的集合对象。
 * List内就是BaseWrapper，不要写成BaseWrapper的泛型实现形式，毕竟有可能数据源在一个List里塞上不同的数据类型
 */
public class ListWrapper extends BaseWrapper<List<BaseWrapper>> {

    private final static DataType DATA_TYPE = BaseDataType.LIST;
    private final static String UNSUPPORTED_MESSAGE = "Unsupported to convert list wrapper to any basic data type, except string.";

    public ListWrapper() {
        super(new ArrayList<>(), DATA_TYPE, 0);
    }

    public ListWrapper(int initialSize) {
        super(new ArrayList<>(initialSize), DATA_TYPE, 0);
    }

    public static final NullWrapper NULL_INSTANCE = NullWrapper.get(DATA_TYPE);

    public void add(BaseWrapper item) {
        item.setParent(this);
        getValue().add(item);
        addByteSize(item.getByteSize());
    }

    public BaseWrapper get(int i) {
        return getValue().get(i);
    }

    public int size() {
        return getValue().size();
    }

    @Override
    public Long asLong() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Double asDouble() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public BigInteger asBigInteger() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Boolean asBoolean() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public Date asDate() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public String asString() {
        return asJson().toJSONString();
    }

    @Override
    public byte[] asBytes() {
        throw new SerializeException(UNSUPPORTED_MESSAGE);
    }

    @Override
    public JSON asJson() {
        return JSONArray.parseArray(JSON.toJSONString(asDefault()));
    }

    @Override
    public Object asDefault() {
        return getValue().stream()
                .map(BaseWrapper::asDefault)
                .collect(Collectors.toList());
    }
}
