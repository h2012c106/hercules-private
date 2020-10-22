package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.google.common.primitives.UnsignedBytes;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class BytesWrapper extends BaseWrapper<byte[]> {

    private final static DataType DATA_TYPE = BaseDataType.BYTES;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode;

    protected BytesWrapper(String value, String encode) {
        this(new byte[0]);
        byte[] bytesValue = null;
        try {
            bytesValue = value.getBytes(encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
        setValue(bytesValue);
        setByteSize(bytesValue.length);
        this.encode = encode;
    }

    protected BytesWrapper(byte[] value, String encode) {
        this(value);
        this.encode = encode;
    }

    protected BytesWrapper(byte[] value) {
        super(value, DATA_TYPE, value.length);
    }

    public static BaseWrapper get(String value, String encode) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new BytesWrapper(value, encode);
    }

    public static BaseWrapper get(String value) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new BytesWrapper(value, DEFAULT_ENCODE);
    }

    public static BaseWrapper get(byte[] value, String encode) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new BytesWrapper(value, encode);
    }

    public static BaseWrapper get(byte[] value) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new BytesWrapper(value);
    }

    @Override
    public InfinitableBigDecimal asBigDecimal() {
        throw new SerializeException("Unsupported to convert bytes to BigDecimal.");
    }

    @Override
    public BigInteger asBigInteger() {
        throw new SerializeException("Unsupported to convert bytes to BigInteger.");
    }

    @Override
    public Boolean asBoolean() {
        throw new SerializeException("Unsupported to convert bytes to boolean.");
    }

    @Override
    public ExtendedDate asDate() {
        throw new SerializeException("Unsupported to convert bytes to date.");
    }

    @Override
    public String asString() {
        try {
            String encode = this.encode == null ? DEFAULT_ENCODE : this.encode;
            return new String(getValue(), encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public byte[] asBytes() {
        return getValue();
    }

    @Override
    public JSON asJson() {
        return parseJson(asString());
    }

    private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

    @Override
    public int compareTo(byte[] o) {
        return COMPARATOR.compare(getValue(), o);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + getType() + ">[" + Arrays.toString(getValue()) + "]";
    }
}
