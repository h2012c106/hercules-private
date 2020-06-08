package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseInputWrapperManager extends WrapperGetterFactory<byte[]> {
    @Override
    protected WrapperGetter<byte[]> getByteGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getShortGetter() {
        return (res, name, seq, al) -> {
            return IntegerWrapper.get(res == null ? null : Bytes.toShort(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getIntegerGetter() {
        return (res, name, seq, al) -> {
            return IntegerWrapper.get(res == null ? null : Bytes.toInt(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getLongGetter() {
        return (res, name, seq, al) -> {
            return IntegerWrapper.get(res == null ? null : Bytes.toLong(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getLonglongGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getFloatGetter() {
        return (res, name, seq, al) -> {
            return DoubleWrapper.get(res == null ? null : Bytes.toFloat(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getDoubleGetter() {
        return (res, name, seq, al) -> {
            return DoubleWrapper.get(res == null ? null : Bytes.toDouble(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getDecimalGetter() {
        return (res, name, seq, al) -> {
            return DoubleWrapper.get(res == null ? null : Bytes.toBigDecimal(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getBooleanGetter() {
        return (res, name, seq, al) -> {
            return BooleanWrapper.get(res == null ? null : Bytes.toBoolean(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getStringGetter() {
        return (res, name, seq, al) -> {
            return StringWrapper.get(res == null ? null : Bytes.toString(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getDateGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getTimeGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getDatetimeGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getBytesGetter() {
        return (res, name, seq, al) -> {
            return BytesWrapper.get(res);
        };
    }

    @Override
    protected WrapperGetter<byte[]> getNullGetter() {
        return (res, name, seq, al) -> NullWrapper.INSTANCE;
    }
}
