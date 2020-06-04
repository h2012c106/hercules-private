package com.xiaohongshu.db.hercules.rdbms.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;

import java.math.BigDecimal;
import java.sql.ResultSet;

public class RDBMSWrapperGetterFactory extends WrapperGetterFactory<ResultSet> {

    @Override
    protected WrapperGetter<ResultSet> getByteGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Byte res = row.getByte(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return IntegerWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getShortGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Short res = row.getShort(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return IntegerWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getIntegerGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Integer res = row.getInt(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return IntegerWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getLongGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Long res = row.getLong(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return IntegerWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getLonglongGetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected WrapperGetter<ResultSet> getFloatGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Float res = row.getFloat(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return DoubleWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDoubleGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Double res = row.getDouble(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return DoubleWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDecimalGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal res = row.getBigDecimal(columnSeq);
                return DoubleWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getBooleanGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                Boolean res = row.getBoolean(columnSeq);
                if (row.wasNull()) {
                    res = null;
                }
                return BooleanWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getStringGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                String res = row.getString(columnSeq);
                return StringWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDateGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                java.sql.Date res = row.getDate(columnSeq);
                return DateWrapper.get(res, BaseDataType.DATE);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getTimeGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                java.sql.Time res = row.getTime(columnSeq);
                return DateWrapper.get(res, BaseDataType.TIME);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getDatetimeGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                String res = SqlUtils.getTimestamp(row, columnSeq);
                return DateWrapper.get(res, BaseDataType.DATETIME);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getBytesGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                byte[] res = row.getBytes(columnSeq);
                return BytesWrapper.get(res);
            }
        };
    }

    @Override
    protected WrapperGetter<ResultSet> getNullGetter() {
        return new WrapperGetter<ResultSet>() {
            @Override
            public BaseWrapper get(ResultSet row, String rowName, String columnName, int columnSeq) throws Exception {
                return NullWrapper.INSTANCE;
            }
        };
    }
}
