package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 使用String型存储
 */
public class DateWrapper extends BaseWrapper<String> {

    public DateWrapper(Long value, DataType type) {
        this(new Date(value), type);
    }

    public DateWrapper(Date value, DataType type) {
        this(DateUtils.dateToString(value, type, DateUtils.getSourceDateFormat()), type);
    }

    /**
     * 强烈建议使用本方法初始化，搭配{@link #asString()}使用，避免0000-00-00 00:00:00的问题
     * 0000-00-00 00:00:00经过dateformat之后会变成0002-11-30 00:00:00且无报错
     *
     * @param value
     */
    public DateWrapper(String value, @NonNull DataType type) {
        super(value, type, value.length());
    }

    @Override
    public Long asLong() {
        return asDate().getTime();
    }

    @Override
    public Double asDouble() {
        return asLong().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal() {
        return BigDecimal.valueOf(asDouble());
    }

    @Override
    public BigInteger asBigInteger() {
        return BigInteger.valueOf(asLong());
    }

    @Override
    public Boolean asBoolean() {
        throw new SerializeException("Unsupported to convert date to boolean.");
    }

    /**
     * 如果日期格式或日期值出错，此处直接抛错
     *
     * @return
     */
    @Override
    public Date asDate() {
        return DateUtils.stringToDate(getValue(), getType(), DateUtils.getSourceDateFormat());
    }

    /**
     * 诸如0000-00-00 00:00:00仅当上下游日期格式一致时会被允许放行
     *
     * @return
     */
    @Override
    public String asString() {
        if (DateUtils.getSourceDateFormat().equals(DateUtils.getTargetDateFormat())) {
            return getValue();
        } else {
            // 当上下游日期格式不一致时，又遇到错误的日期格式/值，则会抛错
            return DateUtils.dateToString(DateUtils.stringToDate(getValue(), getType(), DateUtils.getSourceDateFormat()),
                    getType(),
                    DateUtils.getTargetDateFormat());
        }
    }

    @Override
    public byte[] asBytes() {
        return getValue().getBytes();
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert date to json.");
    }
}
