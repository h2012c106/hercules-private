package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class StringWrapper extends BaseWrapper<String> {

    private final static DataType DATA_TYPE = BaseDataType.STRING;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode = null;

    protected StringWrapper(byte[] value, String encode) {
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

    protected StringWrapper(String value) {
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
    public InfinitableBigDecimal asBigDecimal() {
        return InfinitableBigDecimal.valueOf(new BigDecimal(getValue()));
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
        return ExtendedDate.initialize(DateUtils.stringToDate(getValue(), DateUtils.getSourceDateFormat()));
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

    @Override
    public int compareTo(String o) {
        return getValue().compareTo(o);
    }
}
