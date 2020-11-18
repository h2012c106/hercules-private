package com.xiaohongshu.db.hercules.mongodb.datatype;

import com.alibaba.fastjson.JSON;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.entity.InfinitableBigDecimal;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ObjectIdWrapper extends BaseWrapper<ObjectId> {

    public static BaseWrapper<?> get(ObjectId value) {
        return value == null ? NullWrapper.get(ObjectIdCustomDataType.INSTANCE) : new ObjectIdWrapper(value);
    }

    private ObjectIdWrapper(@NonNull ObjectId value) {
        super(value, ObjectIdCustomDataType.INSTANCE, value.toByteArray().length);
    }

    @Override
    public InfinitableBigDecimal asBigDecimal() {
        return InfinitableBigDecimal.valueOf(new BigDecimal(asBigInteger()));
    }

    @Override
    public BigInteger asBigInteger() {
        return BigInteger.valueOf(getValue().getTimestamp());
    }

    @Override
    public Boolean asBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExtendedDate asDate() {
        return ExtendedDate.initialize(getValue().getDate());
    }

    @Override
    public String asString() {
        return getValue().toHexString();
    }

    @Override
    public byte[] asBytes() {
        return getValue().toByteArray();
    }

    @Override
    public JSON asJson() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(ObjectId o) {
        return getValue().compareTo(o);
    }
}
