package com.xiaohongshu.db.hercules.core.serialize.wrapper;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SerializeException;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 使用String型存储
 */
public class DateWrapper extends BaseWrapper<ExtendedDate> {

    private static final long DATE_SIZE = 32;

    protected DateWrapper(ExtendedDate value, @NonNull DataType type) {
        super(value, type, DATE_SIZE);
    }

    private static BaseWrapper<?> get(ExtendedDate value, DataType type) {
        return value == null ? NullWrapper.get(type) : new DateWrapper(value, type);
    }

    public static BaseWrapper<?> getDate(ExtendedDate value) {
        return get(value, BaseDataType.DATE);
    }

    public static BaseWrapper<?> getTime(ExtendedDate value) {
        return get(value, BaseDataType.TIME);
    }

    public static BaseWrapper<?> getDatetime(ExtendedDate value) {
        return get(value, BaseDataType.DATETIME);
    }

    @Override
    public InfinitableBigDecimal asBigDecimal() {
        return InfinitableBigDecimal.valueOf(new BigDecimal(asBigInteger()));
    }

    @Override
    public BigInteger asBigInteger() {
        return BigInteger.valueOf(asDate().getDate().getTime());
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

    @Override
    public int compareTo(ExtendedDate thatValue) {
        ExtendedDate thisValue = getValue();
        if (thisValue.isZero() && thatValue.isZero()) {
            return 0;
        } else if (thisValue.isZero()) {
            return -1;
        } else if (thatValue.isZero()) {
            return 1;
        } else {
            return thisValue.getDate().compareTo(thatValue.getDate());
        }
    }
}
