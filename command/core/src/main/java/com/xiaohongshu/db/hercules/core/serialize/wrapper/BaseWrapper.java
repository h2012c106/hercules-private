package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import lombok.NonNull;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @param <T> 底层用于存储数据的真正数据类型
 */
public abstract class BaseWrapper<T> implements Comparable<BaseWrapper<?>> {
    /**
     * 不接受任何null值，一个wrapper里永远存着meaningful信息，null值全部扔到{@link NullWrapper}
     */
    private T value;
    private DataType type;
    private long byteSize;
    private BaseWrapper parent = null;

    public BaseWrapper(@NonNull T value, DataType type, long byteSize) {
        this.value = value;
        this.type = type;
        this.byteSize = byteSize;
    }

    protected void setParent(BaseWrapper parent) {
        this.parent = parent;
    }

    protected T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public long getByteSize() {
        return byteSize;
    }

    public boolean isNull() {
        return false;
    }

    private void addParentByteSize(long byteSize) {
        if (parent != null) {
            parent.addByteSize(byteSize);
        }
    }

    public void setByteSize(long byteSize) {
        addParentByteSize(-1 * this.byteSize);
        this.byteSize = byteSize;
        addParentByteSize(byteSize);
    }

    protected void addByteSize(long byteSize) {
        this.byteSize += byteSize;
        addParentByteSize(byteSize);
    }

    public Byte asByte() {
        return asBigDecimal().byteValueExact();
    }

    public Short asShort() {
        return asBigDecimal().shortValueExact();
    }

    public Integer asInteger() {
        return asBigDecimal().intValueExact();
    }

    abstract public Long asLong();

    public Float asFloat() {
        return OverflowUtils.numberToFloat(asBigDecimal());
    }

    abstract public Double asDouble();

    abstract public BigDecimal asBigDecimal();

    abstract public BigInteger asBigInteger();

    abstract public Boolean asBoolean();

    abstract public ExtendedDate asDate();

    abstract public String asString();

    abstract public byte[] asBytes();

    abstract public JSON asJson();

    /**
     * 下游数据源对类型不敏感（无类型），无法提前知道这一列的类型时，直接调这个，返回raw的value。
     * <p>
     * 为什么有泛型T还要返回Object（或者这个方法存在的意义，为什么不直接{@link #getValue()}？
     * 普通类型当然用T即可（且美观），但是wrap类型list或map返回一个BaseWrapper的List没有任何意义，
     * 他们应当返回内部元素asDefault后的List或Map。
     *
     * @return
     */
    public Object asDefault() {
        return getValue();
    }

    public static JSON parseJson(String s) {
        Object res = JSON.parse(s);
        if (res instanceof JSONArray) {
            return (JSONArray) res;
        } else if (res instanceof JSONObject) {
            return (JSONObject) res;
        } else {
            throw new SerializeException("Unknown json parse result class: " + res.getClass().getCanonicalName());
        }
    }

    public static DataType isJsonStrListOrMap(String s) {
        Object res = JSON.parse(s);
        if (res instanceof JSONArray) {
            return BaseDataType.LIST;
        } else if (res instanceof JSONObject) {
            return BaseDataType.MAP;
        } else {
            throw new SerializeException("Unknown json parse result class: " + res.getClass().getCanonicalName());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("value", value)
                .append("type", type)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseWrapper<?> that = (BaseWrapper<?>) o;
        return Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}
