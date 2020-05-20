package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 没有对应的asXXX方法，因为本质上是一个wrapper的集合对象。
 * Map的value就是BaseWrapper，不要写成BaseWrapper的泛型实现形式，毕竟有可能数据源在一个Map value set里塞上不同的数据类型。
 * Map的key为键值可以理解为列名，String即可。
 * <p>
 * 同时作为{@link com.xiaohongshu.db.hercules.core.serialize.HerculesWritable}的内部存储类
 */
public class MapWrapper extends BaseWrapper<Map<String, BaseWrapper>> {

    private final static DataType DATA_TYPE = DataType.MAP;
    private final static String UNSUPPORTED_MESSAGE = "Unsupported to convert map wrapper to any basic data type, except string.";

    public MapWrapper() {
        this(1);
    }

    public MapWrapper(int size) {
        super(new HashMap<>((int) ((float) size / 0.75F + 1.0F)),
                DATA_TYPE,
                0);
    }

    public void put(String columnName, @NonNull BaseWrapper value) {
        BaseWrapper prevValue = get(columnName);
        if (prevValue != null) {
            addByteSize(-1 * prevValue.getByteSize());
        }
        value.setParent(this);
        getValue().put(columnName, value);
        addByteSize(value.getByteSize());
    }

    public BaseWrapper get(String columnName) {
        return getValue().get(columnName);
    }

    public Set<Map.Entry<String, BaseWrapper>> entrySet() {
        return getValue().entrySet();
    }

    public boolean containsColumn(String columnName) {
        return getValue().containsKey(columnName);
    }

    public BaseWrapper remove(String columnName) {
        BaseWrapper removedValue = getValue().remove(columnName);
        if (removedValue != null) {
            addByteSize(-1 * removedValue.getByteSize());
        }
        return removedValue;
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
        return JSONObject.parseObject(JSON.toJSONString(asDefault()));
    }

    @Override
    public Object asDefault() {
        return getValue().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().asDefault()));
    }
}
