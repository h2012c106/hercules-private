package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 没有对应的asXXX方法，因为本质上是一个wrapper的集合对象。
 * Map的value就是BaseWrapper，不要写成BaseWrapper的泛型实现形式，毕竟有可能数据源在一个Map value set里塞上不同的数据类型。
 * Map的key为键值可以理解为列名，String即可。
 * <p>
 * 同时作为{@link HerculesWritable}的内部存储类
 */
public class MapWrapper extends BaseWrapper<Map<String, BaseWrapper<?>>> implements Map<String, BaseWrapper<?>> {

    private final static DataType DATA_TYPE = BaseDataType.MAP;
    private final static String UNSUPPORTED_MESSAGE = "Unsupported to convert map wrapper to any basic data type, except string.";

    public MapWrapper() {
        this(1);
    }

    public MapWrapper(int size) {
        super(new LinkedHashMap<>((int) ((float) size / 0.75F + 1.0F)),
                DATA_TYPE,
                0);
    }

    public static final NullWrapper NULL_INSTANCE = NullWrapper.get(DATA_TYPE);

    @Override
    public int size() {
        return getValue().size();
    }

    @Override
    public boolean isEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getValue().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getValue().containsValue(value);
    }

    @Override
    public BaseWrapper<?> get(Object key) {
        return getValue().get(key);
    }

    @Override
    public BaseWrapper<?> put(String columnName, @NonNull BaseWrapper<?> value) {
        BaseWrapper<?> prevValue = getValue().put(columnName, value);
        value.setParent(this);
        if (prevValue != null) {
            addByteSize(-1 * prevValue.getByteSize());
        }
        addByteSize(value.getByteSize());
        return prevValue;
    }

    @Override
    public BaseWrapper<?> remove(Object key) {
        BaseWrapper<?> removedValue = getValue().remove(key);
        if (removedValue != null) {
            addByteSize(-1 * removedValue.getByteSize());
        }
        return removedValue;
    }

    @Override
    public void putAll(Map<? extends String, ? extends BaseWrapper<?>> m) {
        for (Map.Entry<? extends String, ? extends BaseWrapper<?>> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        for (String key : new HashSet<>(getValue().keySet())) {
            remove(key);
        }
    }

    @Override
    public Set<String> keySet() {
        return getValue().keySet();
    }

    @Override
    public Collection<BaseWrapper<?>> values() {
        return getValue().values();
    }

    @Override
    public Set<Map.Entry<String, BaseWrapper<?>>> entrySet() {
        return getValue().entrySet();
    }

    @Override
    public InfinitableBigDecimal asBigDecimal() {
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
    public ExtendedDate asDate() {
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
        Map<String, Object> map = new HashMap<>(size());
        for(Map.Entry<String, BaseWrapper<?>> entry : getValue().entrySet()){
            String key = entry.getKey();
            Object value = entry.getValue().asDefault();
            map.put(key, value);
        }
        return map;
    }

    @Override
    public int compareTo(Map<String, BaseWrapper<?>> o) {
        throw new UnsupportedOperationException();
    }
}
