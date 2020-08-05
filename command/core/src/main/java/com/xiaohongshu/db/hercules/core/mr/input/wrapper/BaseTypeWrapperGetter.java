package com.xiaohongshu.db.hercules.core.mr.input.wrapper;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class BaseTypeWrapperGetter<T, I> extends WrapperGetter<I> {

    abstract protected T getNonnullValue(I row, String rowName, String columnName, int columnSeq) throws Exception;

    @Override
    protected BaseWrapper<?> getNonnull(I row, String rowName, String columnName, int columnSeq) throws Exception {
        return getType().getBaseDataType().getReadFunction().apply(getNonnullValue(row, rowName, columnName, columnSeq));
    }

    public static abstract class ByteGetter<I> extends BaseTypeWrapperGetter<Byte, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.BYTE;
        }
    }

    public static abstract class ShortGetter<I> extends BaseTypeWrapperGetter<Short, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.SHORT;
        }
    }

    public static abstract class IntegerGetter<I> extends BaseTypeWrapperGetter<Integer, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.INTEGER;
        }
    }

    public static abstract class LongGetter<I> extends BaseTypeWrapperGetter<Long, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.LONG;
        }
    }

    public static abstract class LonglongGetter<I> extends BaseTypeWrapperGetter<BigInteger, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.LONGLONG;
        }
    }

    public static abstract class FloatGetter<I> extends BaseTypeWrapperGetter<Float, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.FLOAT;
        }
    }

    public static abstract class DoubleGetter<I> extends BaseTypeWrapperGetter<Double, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.DOUBLE;
        }
    }

    public static abstract class DecimalGetter<I> extends BaseTypeWrapperGetter<BigDecimal, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.DECIMAL;
        }
    }

    public static abstract class BooleanGetter<I> extends BaseTypeWrapperGetter<Boolean, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.BOOLEAN;
        }
    }

    public static abstract class DateGetter<I> extends BaseTypeWrapperGetter<ExtendedDate, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.DATE;
        }
    }

    public static abstract class TimeGetter<I> extends BaseTypeWrapperGetter<ExtendedDate, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.TIME;
        }
    }

    public static abstract class DatetimeGetter<I> extends BaseTypeWrapperGetter<ExtendedDate, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.DATETIME;
        }
    }

    public static abstract class StringGetter<I> extends BaseTypeWrapperGetter<String, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.STRING;
        }
    }

    public static abstract class BytesGetter<I> extends BaseTypeWrapperGetter<byte[], I> {
        @Override
        protected DataType getType() {
            return BaseDataType.BYTES;
        }
    }

    public static abstract class NullGetter<I> extends BaseTypeWrapperGetter<Void, I> {
        @Override
        protected DataType getType() {
            return BaseDataType.NULL;
        }
    }

}
