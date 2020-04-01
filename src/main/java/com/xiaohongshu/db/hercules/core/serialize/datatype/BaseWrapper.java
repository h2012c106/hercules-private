package com.xiaohongshu.db.hercules.core.serialize.datatype;

import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * @param <T> 底层用于存储数据的真正数据类型
 */
public abstract class BaseWrapper<T> {
    /**
     * 不接受任何null值，一个wrapper里永远存着meaningful信息，null值全部扔到{@link NullWrapper}
     */
    private T value;
    private DataType type;
    private int byteSize;

    public BaseWrapper(@NonNull T value, DataType type, int byteSize) {
        this.value = value;
        this.type = type;
        this.byteSize = byteSize;
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

    public int getByteSize() {
        return byteSize;
    }

    public void setByteSize(int byteSize) {
        this.byteSize = byteSize;
    }

    abstract public Long asLong();

    abstract public Double asDouble();

    abstract public BigDecimal asBigDecimal();

    abstract public BigInteger asBigInteger();

    abstract public Boolean asBoolean();

    abstract public Date asDate();

    abstract public String asString();

    abstract public byte[] asBytes();
}
