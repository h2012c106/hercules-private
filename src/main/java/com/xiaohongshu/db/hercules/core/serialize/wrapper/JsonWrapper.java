package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 此Wrapper仅在某个字段真正是json万不得已的时候使用，虽然看似可以某种程度替代{@link ListWrapper}和{@link MapWrapper}
 * 但是如果无脑使用这个，势必会损失类型信息，下游从json中读出类型信息不是不可以，但是有两点坏处：
 * 一是势必会损失一定的类型信息，比如Date型；
 * 二是造成下游的parse类型的逻辑冗余——既包括从hercules标准wrapper中的parse，又包括从json类型中的parse。
 */
@Deprecated
public class JsonWrapper extends BaseWrapper<JSON> {

    private final static DataType DATA_TYPE = null;

    private final static String DEFAULT_ENCODE = "UTF-8";

    private String encode = null;

    public JsonWrapper(byte[] value) {
        this(value, DEFAULT_ENCODE);
    }

    public JsonWrapper(byte[] value, String encode) {
        this(new JSONObject());
        String strValue = null;
        try {
            strValue = new String(value, encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
        setValue(parseJson(strValue));
        setByteSize(value.length);
        this.encode = encode;
    }

    public JsonWrapper(String value) {
        this(parseJson(value), value.length());
    }

    public JsonWrapper(JSON value) {
        this(value, value.toJSONString().length());
    }

    private JsonWrapper(JSON value, int length) {
        super(value, DATA_TYPE, length);
    }

    @Override
    public Long asLong() {
        throw new SerializeException("Unsupported to convert json to number.");
    }

    @Override
    public Double asDouble() {
        throw new SerializeException("Unsupported to convert json to number.");
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw new SerializeException("Unsupported to convert json to number.");
    }

    @Override
    public BigInteger asBigInteger() {
        throw new SerializeException("Unsupported to convert json to number.");
    }

    @Override
    public Boolean asBoolean() {
        throw new SerializeException("Unsupported to convert json to boolean.");
    }

    @Override
    public Date asDate() {
        throw new SerializeException("Unsupported to convert json to date.");
    }

    @Override
    public String asString() {
        return getValue().toJSONString();
    }

    @Override
    public byte[] asBytes() {
        try {
            String encode = this.encode == null ? DEFAULT_ENCODE : this.encode;
            return asString().getBytes(encode);
        } catch (UnsupportedEncodingException e) {
            throw new SerializeException(e);
        }
    }

    @Override
    public JSON asJson() {
        return getValue();
    }
}
