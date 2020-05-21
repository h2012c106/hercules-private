package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseInputWrapperManager extends WrapperGetterFactory<byte[]> {
    @Override
    protected WrapperGetter<byte[]> getByteGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getShortGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new IntegerWrapper(Bytes.toShort(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getIntegerGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new IntegerWrapper(Bytes.toInt(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getLongGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new IntegerWrapper(Bytes.toLong(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getLonglongGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<byte[]> getFloatGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new DoubleWrapper(Bytes.toFloat(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getDoubleGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new DoubleWrapper(Bytes.toDouble(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getDecimalGetter() {
        return (res, name, seq, al) -> {
            if(null==res){
                return new NullWrapper();
            }
            return new DoubleWrapper(Bytes.toBigDecimal(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getBooleanGetter() {
        return (res, name, seq, al) -> {
            if (res==null) {
                return new NullWrapper();
            }
            return new BooleanWrapper(Bytes.toBoolean(res));
        };
    }

    @Override
    protected WrapperGetter<byte[]> getStringGetter() {
        return (res, name, seq, al) -> {
            if (res==null) {
                return new NullWrapper();
            }
            return new StringWrapper(Bytes.toString(res));
        };
    }

    // TODO 检查目前的转换能否正常work，借鉴自 dataX
    @Override
    protected WrapperGetter<byte[]> getDateGetter() {
        return (res, name, seq, al) -> {
            if (res==null) {
                return new NullWrapper();
            } else {
                String dateValue = Bytes.toStringBinary(res);
                // 需要设定一个dateformat，即 new String()
                return new DateWrapper(DateUtils.parseDate(dateValue, new String()), DataType.DATE);
            }
        };
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
            if (res==null) {
                return new NullWrapper();
            }
            return new BytesWrapper(res);
        };
    }

    @Override
    protected WrapperGetter<byte[]> getNullGetter() {
        return (res, name, seq, al) -> NullWrapper.INSTANCE;
    }
}
