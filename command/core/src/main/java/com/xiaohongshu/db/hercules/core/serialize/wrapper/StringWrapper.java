package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class StringWrapper extends BaseWrapper<String> {

    private final static DataType DATA_TYPE = BaseDataType.STRING;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode = null;

    private StringWrapper(byte[] value, String encode) {
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

    private StringWrapper(String value) {
        super(value, DATA_TYPE, value.length());
    }

    public static BaseWrapper get(byte[] value, String encode) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new StringWrapper(value, encode);
    }

    public static BaseWrapper get(byte[] value) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new StringWrapper(value, DEFAULT_ENCODE);
    }

    public static BaseWrapper get(String value) {
        return value == null ? NullWrapper.get(DATA_TYPE) : new StringWrapper(value);
    }

    @Override
    public Long asLong() {
        return asBigInteger().longValueExact();
    }

    @Override
    public Double asDouble() {
        BigDecimal tmpDecimal = asBigDecimal();
        return OverflowUtils.numberToDouble(tmpDecimal);
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
    public ExtendedDate asDate() {
        return new ExtendedDate(DateUtils.stringToDate(getValue(), DateUtils.getSourceDateFormat()));
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
