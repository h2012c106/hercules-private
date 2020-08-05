package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * 使用String型存储
 */
public class DateWrapper extends BaseWrapper<ExtendedDate> {

    private static final long DATE_SIZE = 32;

    private DateWrapper(ExtendedDate value, @NonNull DataType type) {
        super(value, type, DATE_SIZE);
    }

    private static BaseWrapper<?> get(ExtendedDate value, DataType type) {
        return value == null ? NullWrapper.get(type) : new DateWrapper(value, type);
    }

    public static BaseWrapper<?> getDate(ExtendedDate value){
        return get(value, BaseDataType.DATE);
    }

    public static BaseWrapper<?> getTime(ExtendedDate value){
        return get(value, BaseDataType.TIME);
    }

    public static BaseWrapper<?> getDatetime(ExtendedDate value){
        return get(value, BaseDataType.DATETIME);
    }

    @Override
    public Long asLong() {
        return asDate().getDate().getTime();
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
    public ExtendedDate asDate() {
        return getValue();
    }

    /**
     * 诸如0000-00-00 00:00:00仅当上下游日期格式一致时会被允许放行
     *
     * @return
     */
    @Override
    public String asString() {
        if (!getValue().isZero()) {
            return DateUtils.dateToString(getValue().getDate(), getType().getBaseDataType(), DateUtils.getTargetDateFormat());
        } else {
            return DateUtils.ZERO_DATE;
        }
    }

    @Override
    public byte[] asBytes() {
        throw new SerializeException("Unsupported to convert date to bytes.");
    }

    @Override
    public JSON asJson() {
        throw new SerializeException("Unsupported to convert date to json.");
    }
}
