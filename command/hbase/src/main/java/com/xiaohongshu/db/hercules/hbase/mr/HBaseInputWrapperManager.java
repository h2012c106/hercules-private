package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

/**
 * Caution: 这里的byte[]泛型对于框架而言是不规范的，原则上而言这里的泛型实现应该是代表行的结构，而byte[]为列结构，此处work仅因为hbase里任何数据取出来都是一个byte[]。
 * 一般而言，行的数据结构一定是确定的，而列结构会因为不同的数据类型而变化。
 */
public class HBaseInputWrapperManager extends WrapperGetterFactory<byte[]> {
    @Override
    protected BaseTypeWrapperGetter.ByteGetter<byte[]> getByteGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.ShortGetter<byte[]> getShortGetter() {
        return new BaseTypeWrapperGetter.ShortGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Short getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toShort(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.IntegerGetter<byte[]> getIntegerGetter() {
        return new BaseTypeWrapperGetter.IntegerGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Integer getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toInt(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LongGetter<byte[]> getLongGetter() {
        return new BaseTypeWrapperGetter.LongGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Long getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toLong(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LonglongGetter<byte[]> getLonglongGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.FloatGetter<byte[]> getFloatGetter() {
        return new BaseTypeWrapperGetter.FloatGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Float getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toFloat(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DoubleGetter<byte[]> getDoubleGetter() {
        return new BaseTypeWrapperGetter.DoubleGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Double getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toDouble(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DecimalGetter<byte[]> getDecimalGetter() {
        return new BaseTypeWrapperGetter.DecimalGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected BigDecimal getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toBigDecimal(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BooleanGetter<byte[]> getBooleanGetter() {
        return new BaseTypeWrapperGetter.BooleanGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected Boolean getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toBoolean(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.StringGetter<byte[]> getStringGetter() {
        return new BaseTypeWrapperGetter.StringGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected String getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return Bytes.toString(row);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DateGetter<byte[]> getDateGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<byte[]> getTimeGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<byte[]> getDatetimeGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.BytesGetter<byte[]> getBytesGetter() {
        return new BaseTypeWrapperGetter.BytesGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row == null;
            }

            @Override
            protected byte[] getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return row;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.NullGetter<byte[]> getNullGetter() {
        return new BaseTypeWrapperGetter.NullGetter<byte[]>() {
            @Override
            protected boolean isNull(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return true;
            }

            @Override
            protected Void getNonnullValue(byte[] row, String rowName, String columnName, int columnSeq) throws Exception {
                return null;
            }
        };
    }
}
