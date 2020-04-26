package com.xiaohongshu.db.hercules.core.serialize.datatype;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class StringWrapper extends BaseWrapper<String> {

    private final static DataType DATA_TYPE = DataType.STRING;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode = null;

    public StringWrapper(byte[] value) {
        this(value, DEFAULT_ENCODE);
    }

    public StringWrapper(byte[] value, String encode) {
        this("");
        String strValue = null;
        try {
            strValue = new String(value, encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
        setValue(strValue);
        setByteSize(value.length);
        this.encode = encode;
    }

    public StringWrapper(String value) {
        super(value, DATA_TYPE, value.length());
    }

    @Override
    public Long asLong() {
        BigInteger tmpInteger = new BigInteger(getValue());
        OverflowUtils.validateLong(tmpInteger);
        return tmpInteger.longValue();
    }

    @Override
    public Double asDouble() {
        BigDecimal tmpDecimal = new BigDecimal(getValue());
        OverflowUtils.validateDouble(tmpDecimal);
        return tmpDecimal.doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal() {
        return new BigDecimal(getValue());
    }

    @Override
    public BigInteger asBigInteger() {
        return new BigInteger(getValue());
    }

    @Override
    public Boolean asBoolean() {
        return Boolean.parseBoolean(getValue());
    }

    @Override
    public Date asDate() {
        return DateUtils.stringToDate(getValue(), DateUtils.getSourceDateFormat()).getValue();
    }

    @Override
    public String asString() {
        return getValue();
    }

    @Override
    public byte[] asBytes() {
        try {
            String encode = this.encode == null ? DEFAULT_ENCODE : this.encode;
            return getValue().getBytes(encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public JSON asJson() {
        return parseJson(getValue());
    }
}
