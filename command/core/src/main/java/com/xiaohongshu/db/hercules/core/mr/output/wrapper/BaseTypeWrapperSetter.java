package com.xiaohongshu.db.hercules.core.mr.output.wrapper;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @param <T> 各种数据类型的java类型
 * @param <O> 各种数据源写出时代表一行的数据结构
 */
public abstract class BaseTypeWrapperSetter<T, O> extends WrapperSetter<O> {

    abstract protected DataType getType();

    private T getValue(BaseWrapper<?> wrapper) {
        return (T) getType().getWriteFunction().apply(wrapper);
    }

    abstract protected void setNonnullValue(T value, O row, String rowName, String columnName, int columnSeq) throws Exception;

    @Override
    protected void setNonnull(@NonNull BaseWrapper<?> wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        setNonnullValue(getValue(wrapper), row, rowName, columnName, columnSeq);
    }

    public static abstract class ByteSetter<O> extends BaseTypeWrapperSetter<Byte, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.BYTE;
        }
    }

    public static abstract class ShortSetter<O> extends BaseTypeWrapperSetter<Short, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.SHORT;
        }
    }

    public static abstract class IntegerSetter<O> extends BaseTypeWrapperSetter<Integer, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.INTEGER;
        }
    }

    public static abstract class LongSetter<O> extends BaseTypeWrapperSetter<Long, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.LONG;
        }
    }

    public static abstract class LonglongSetter<O> extends BaseTypeWrapperSetter<BigInteger, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.LONGLONG;
        }
    }

    public static abstract class FloatSetter<O> extends BaseTypeWrapperSetter<Float, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.FLOAT;
        }
    }

    public static abstract class DoubleSetter<O> extends BaseTypeWrapperSetter<Double, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.DOUBLE;
        }
    }

    public static abstract class DecimalSetter<O> extends BaseTypeWrapperSetter<BigDecimal, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.DECIMAL;
        }
    }

    public static abstract class BooleanSetter<O> extends BaseTypeWrapperSetter<Boolean, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.BOOLEAN;
        }
    }

    public static abstract class DateSetter<O> extends BaseTypeWrapperSetter<ExtendedDate, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.DATE;
        }
    }

    public static abstract class TimeSetter<O> extends BaseTypeWrapperSetter<ExtendedDate, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.TIME;
        }
    }

    public static abstract class DatetimeSetter<O> extends BaseTypeWrapperSetter<ExtendedDate, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.DATETIME;
        }
    }

    public static abstract class StringSetter<O> extends BaseTypeWrapperSetter<String, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.STRING;
        }
    }

    public static abstract class BytesSetter<O> extends BaseTypeWrapperSetter<byte[], O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.BYTES;
        }
    }

    public static abstract class NullSetter<O> extends BaseTypeWrapperSetter<Void, O> {
        @Override
        protected final DataType getType() {
            return BaseDataType.NULL;
        }
    }

}
