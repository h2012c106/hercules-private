package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 没有对应的asXXX方法，因为本质上是一个wrapper的集合对象。
 * List内就是BaseWrapper，不要写成BaseWrapper的泛型实现形式，毕竟有可能数据源在一个List里塞上不同的数据类型
 */
public class ListWrapper extends BaseWrapper<List<BaseWrapper<?>>> implements List<BaseWrapper<?>> {

    private final static DataType DATA_TYPE = BaseDataType.LIST;
    private final static String UNSUPPORTED_MESSAGE = "Unsupported to convert list wrapper to any basic data type, except string.";

    public ListWrapper() {
        super(new ArrayList<>(), DATA_TYPE, 0);
    }

    public ListWrapper(int initialSize) {
        super(new ArrayList<>(initialSize), DATA_TYPE, 0);
    }

    public static final NullWrapper NULL_INSTANCE = NullWrapper.get(DATA_TYPE);

    @Override
    public boolean add(BaseWrapper<?> item) {
        item.setParent(this);
        getValue().add(item);
        addByteSize(item.getByteSize());
        return true;
    }

    @Override
    public void add(int index, BaseWrapper<?> element) {
        element.setParent(this);
        getValue().add(index, element);
        addByteSize(element.getByteSize());
    }

    @Override
    public boolean addAll(Collection<? extends BaseWrapper<?>> c) {
        boolean res = getValue().addAll(c);
        for (BaseWrapper<?> item : c) {
            addByteSize(item.getByteSize());
        }
        return res;
    }

    @Override
    public boolean addAll(int index, Collection<? extends BaseWrapper<?>> c) {
        boolean res = getValue().addAll(index, c);
        for (BaseWrapper<?> item : c) {
            addByteSize(item.getByteSize());
        }
        return res;
    }

    @Override
    public boolean remove(Object o) {
        boolean res = getValue().remove(o);
        if (res) {
            addByteSize(-1 * ((BaseWrapper<?>) o).getByteSize());
        }
        return res;
    }

    @Override
    public BaseWrapper<?> remove(int index) {
        BaseWrapper<?> res = getValue().remove(index);
        if (res != null) {
            addByteSize(-1 * res.getByteSize());
        }
        return res;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean res = false;
        for (Object item : c) {
            if (remove(item)) {
                res = true;
            }
        }
        return res;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean res = false;
        for (BaseWrapper<?> item : new ArrayList<>(getValue())) {
            if (!c.contains(item)) {
                res = true;
                remove(item);
            }
        }
        return res;
    }

    @Override
    public boolean contains(Object o) {
        return getValue().contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return getValue().containsAll(c);
    }

    @Override
    public void clear() {
        for (BaseWrapper<?> item : new ArrayList<>(getValue())) {
            remove(item);
        }
    }

    @Override
    public BaseWrapper<?> get(int i) {
        return getValue().get(i);
    }

    @Override
    public BaseWrapper<?> set(int index, BaseWrapper<?> element) {
        BaseWrapper<?> prevValue = getValue().set(index, element);
        element.setParent(this);
        if (prevValue != null) {
            addByteSize(-1 * prevValue.getByteSize());
        }
        addByteSize(element.getByteSize());
        return prevValue;
    }

    @Override
    public int indexOf(Object o) {
        return getValue().indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return getValue().lastIndexOf(o);
    }

    @Override
    public Iterator<BaseWrapper<?>> iterator() {
        return getValue().iterator();
    }

    @Override
    public ListIterator<BaseWrapper<?>> listIterator() {
        return getValue().listIterator();
    }

    @Override
    public ListIterator<BaseWrapper<?>> listIterator(int index) {
        return getValue().listIterator(index);
    }

    @Override
    public List<BaseWrapper<?>> subList(int fromIndex, int toIndex) {
        return getValue().subList(fromIndex, toIndex);
    }

    @Override
    public int size() {
        return getValue().size();
    }

    @Override
    public boolean isEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public Object[] toArray() {
        return getValue().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return getValue().toArray(a);
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
        return JSONArray.parseArray(JSON.toJSONString(asDefault()));
    }

    @Override
    public Object asDefault() {
        List<Object> list = new ArrayList<>();
        for(BaseWrapper<?> wrapper : getValue()){
            list.add(wrapper.asDefault());
        }
        return list;
    }

    @Override
    public int compareTo(List<BaseWrapper<?>> o) {
        throw new UnsupportedOperationException();
    }
}
