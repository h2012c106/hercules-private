package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;

public class RDBMSWrapperGetterFactory extends WrapperGetterFactory<ResultSet> {

    public RDBMSWrapperGetterFactory() {
        super(DataSourceRole.SOURCE);
    }

    @Override
    protected BaseTypeWrapperGetter.ByteGetter<ResultSet> getByteGetter() {
        return new BaseTypeWrapperGetter.ByteGetter<ResultSet>() {
            @Override
            protected Byte getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getByte(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getByte(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.ShortGetter<ResultSet> getShortGetter() {
        return new BaseTypeWrapperGetter.ShortGetter<ResultSet>() {
            @Override
            protected Short getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getShort(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getShort(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.IntegerGetter<ResultSet> getIntegerGetter() {
        return new BaseTypeWrapperGetter.IntegerGetter<ResultSet>() {
            @Override
            protected Integer getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getInt(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getInt(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LongGetter<ResultSet> getLongGetter() {
        return new BaseTypeWrapperGetter.LongGetter<ResultSet>() {
            @Override
            protected Long getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getLong(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getLong(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LonglongGetter<ResultSet> getLonglongGetter() {
        return new BaseTypeWrapperGetter.LonglongGetter<ResultSet>() {
            @Override
            protected BigInteger getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return new BigInteger(row.getString(columnSeq));
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getString(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.FloatGetter<ResultSet> getFloatGetter() {
        return new BaseTypeWrapperGetter.FloatGetter<ResultSet>() {
            @Override
            protected Float getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getFloat(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getFloat(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DoubleGetter<ResultSet> getDoubleGetter() {
        return new BaseTypeWrapperGetter.DoubleGetter<ResultSet>() {
            @Override
            protected Double getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getDouble(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getDouble(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DecimalGetter<ResultSet> getDecimalGetter() {
        return new BaseTypeWrapperGetter.DecimalGetter<ResultSet>() {
            @Override
            protected BigDecimal getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getBigDecimal(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getBigDecimal(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BooleanGetter<ResultSet> getBooleanGetter() {
        return new BaseTypeWrapperGetter.BooleanGetter<ResultSet>() {
            @Override
            protected Boolean getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getBoolean(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                row.getBoolean(columnSeq);
                return row.wasNull();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.StringGetter<ResultSet> getStringGetter() {
        return new BaseTypeWrapperGetter.StringGetter<ResultSet>() {
            @Override
            protected String getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getString(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getString(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DateGetter<ResultSet> getDateGetter() {
        return new BaseTypeWrapperGetter.DateGetter<ResultSet>() {
            @Override
            protected ExtendedDate getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.getDate(columnSeq));
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getDate(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<ResultSet> getTimeGetter() {
        return new BaseTypeWrapperGetter.TimeGetter<ResultSet>() {
            @Override
            protected ExtendedDate getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.getTime(columnSeq));
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getTime(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<ResultSet> getDatetimeGetter() {
        return new BaseTypeWrapperGetter.DatetimeGetter<ResultSet>() {
            @Override
            protected ExtendedDate getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return SqlUtils.getTimestamp(row, columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return SqlUtils.getTimestamp(row, columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BytesGetter<ResultSet> getBytesGetter() {
        return new BaseTypeWrapperGetter.BytesGetter<ResultSet>() {
            @Override
            protected byte[] getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getBytes(columnSeq);
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getBytes(columnSeq) == null;
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.NullGetter<ResultSet> getNullGetter() {
        return new BaseTypeWrapperGetter.NullGetter<ResultSet>() {
            @Override
            protected Void getNonnullValue(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return null;
            }

            @Override
            protected boolean isNull(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return true;
            }
        };
    }
}
