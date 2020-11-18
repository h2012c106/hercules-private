package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;

public class HBaseOutputWrapperManager extends WrapperSetterFactory<Put> {
    public HBaseOutputWrapperManager() {
        super(DataSourceRole.TARGET);
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<Put> getByteSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<Put> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Short value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<Put> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Integer value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<Put> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Long value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<Put> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<Put> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Float value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<Put> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Double value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<Put> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(BigDecimal value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<Put> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Boolean value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<Put> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(String value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), Bytes.toBytes(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<Put> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<Put> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<Put> getDatetimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<Put> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<Put>() {
            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(byte[] value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
                row.addColumn(rowName.getBytes(), columnName.getBytes(), value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<Put> getNullSetter() {
        return new BaseTypeWrapperSetter.NullSetter<Put>() {
            @Override
            protected void setNonnullValue(Void value, Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNull(Put row, String rowName, String columnName, int columnSeq) throws Exception {
            }
        };
    }
}
