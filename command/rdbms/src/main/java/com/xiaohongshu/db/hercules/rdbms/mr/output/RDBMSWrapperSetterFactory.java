package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;

public class RDBMSWrapperSetterFactory extends WrapperSetterFactory<PreparedStatement> {

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<PreparedStatement> getByteSetter() {
        return new BaseTypeWrapperSetter.ByteSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.TINYINT);
            }

            @Override
            protected void setNonnullValue(Byte value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setByte(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<PreparedStatement> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.SMALLINT);
            }

            @Override
            protected void setNonnullValue(Short value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setShort(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<PreparedStatement> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.INTEGER);
            }

            @Override
            protected void setNonnullValue(Integer value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setInt(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<PreparedStatement> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.BIGINT);
            }

            @Override
            protected void setNonnullValue(Long value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setLong(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<PreparedStatement> getLonglongSetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<PreparedStatement> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.FLOAT);
            }

            @Override
            protected void setNonnullValue(Float value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setFloat(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<PreparedStatement> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.DOUBLE);
            }

            @Override
            protected void setNonnullValue(Double value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setDouble(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<PreparedStatement> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.DECIMAL);
            }

            @Override
            protected void setNonnullValue(BigDecimal value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setBigDecimal(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<PreparedStatement> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.BOOLEAN);
            }

            @Override
            protected void setNonnullValue(Boolean value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setBoolean(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<PreparedStatement> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.VARCHAR);
            }

            @Override
            protected void setNonnullValue(String value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setString(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<PreparedStatement> getDateSetter() {
        return new BaseTypeWrapperSetter.DateSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.DATE);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setDate(columnSeq, new java.sql.Date(value.getDate().getTime()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<PreparedStatement> getTimeSetter() {
        return new BaseTypeWrapperSetter.TimeSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.TIME);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setTime(columnSeq, new java.sql.Time(value.getDate().getTime()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<PreparedStatement> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.TIMESTAMP);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                if (value.isZero()) {
                    row.setString(columnSeq, DateUtils.ZERO_DATE);
                } else {
                    row.setTimestamp(columnSeq, new Timestamp(value.getDate().getTime()));
                }
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<PreparedStatement> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.LONGVARBINARY);
            }

            @Override
            protected void setNonnullValue(byte[] value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setBytes(columnSeq, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<PreparedStatement> getNullSetter() {
        return new BaseTypeWrapperSetter.NullSetter<PreparedStatement>() {
            @Override
            protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.NULL);
            }

            @Override
            protected void setNonnullValue(Void value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.NULL);
            }
        };
    }
}
