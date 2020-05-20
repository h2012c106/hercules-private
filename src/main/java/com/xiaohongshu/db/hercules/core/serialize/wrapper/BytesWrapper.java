package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

public class BytesWrapper extends BaseWrapper<byte[]> {

    private final static DataType DATA_TYPE = DataType.BYTES;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode;

    public BytesWrapper(String value) {
        this(value, DEFAULT_ENCODE);
    }

    public BytesWrapper(String value, String encode) {
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

    public BytesWrapper(byte[] value, String encode) {
        this(value);
        this.encode = encode;
    }

    public BytesWrapper(byte[] value) {
        super(value, DATA_TYPE, value.length);
    }

    @Override
    public Long asLong() {
        byte[] bytes = getValue();
        if (bytes.length == 4) {
            return (long) (bytes[3] & 0xFF |
                    (bytes[2] & 0xFF) << 8 |
                    (bytes[1] & 0xFF) << 16 |
                    (bytes[0] & 0xFF) << 24);
        } else if (bytes.length == 8) {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.put(bytes, 0, bytes.length);
            buffer.flip();
            return buffer.getLong();
        } else {
            throw new SerializeException("Unable to convert bytes to long: " + Arrays.toString(bytes));
        }
    }

    @Override
    public Double asDouble() {
        throw new SerializeException("Unsupported to convert bytes to double.");
    }

    @Override
    public BigDecimal asBigDecimal() {
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
    public Date asDate() {
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
}
