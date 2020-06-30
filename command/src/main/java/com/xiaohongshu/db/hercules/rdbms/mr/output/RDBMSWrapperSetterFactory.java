package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Date;

public class RDBMSWrapperSetterFactory extends WrapperSetterFactory<PreparedStatement> {

    @Override
    protected WrapperSetter<PreparedStatement> getByteSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res == null) {
                    row.setNull(columnSeq, Types.TINYINT);
                } else {
                    row.setByte(columnSeq, res.byteValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getShortSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res == null) {
                    row.setNull(columnSeq, Types.SMALLINT);
                } else {
                    row.setShort(columnSeq, res.shortValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getIntegerSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res == null) {
                    row.setNull(columnSeq, Types.INTEGER);
                } else {
                    row.setInt(columnSeq, res.intValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getLongSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res == null) {
                    row.setNull(columnSeq, Types.BIGINT);
                } else {
                    row.setLong(columnSeq, res.longValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getLonglongSetter() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected WrapperSetter<PreparedStatement> getFloatSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal res = wrapper.asBigDecimal();
                if (res == null) {
                    row.setNull(columnSeq, Types.FLOAT);
                } else {
                    row.setFloat(columnSeq, OverflowUtils.numberToFloat(res));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDoubleSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal res = wrapper.asBigDecimal();
                if (res == null) {
                    row.setNull(columnSeq, Types.DOUBLE);
                } else {
                    row.setDouble(columnSeq, OverflowUtils.numberToDouble(res));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDecimalSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal res = wrapper.asBigDecimal();
                if (res == null) {
                    row.setNull(columnSeq, Types.DECIMAL);
                } else {
                    row.setBigDecimal(columnSeq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBooleanSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                Boolean res = wrapper.asBoolean();
                if (res == null) {
                    row.setNull(columnSeq, Types.BOOLEAN);
                } else {
                    row.setBoolean(columnSeq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getStringSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                String res = wrapper.asString();
                if (res == null) {
                    row.setNull(columnSeq, Types.VARCHAR);
                } else {
                    row.setString(columnSeq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDateSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                Date res = wrapper.asDate();
                if (res == null) {
                    row.setNull(columnSeq, Types.DATE);
                } else {
                    row.setDate(columnSeq, new java.sql.Date(res.getTime()));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getTimeSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                Date res = wrapper.asDate();
                if (res == null) {
                    row.setNull(columnSeq, Types.TIME);
                } else {
                    row.setTime(columnSeq, new java.sql.Time(res.getTime()));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDatetimeSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                String res = wrapper.asString();
                if (res == null) {
                    row.setNull(columnSeq, Types.TIMESTAMP);
                } else {
                    row.setString(columnSeq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBytesSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                byte[] res = wrapper.asBytes();
                if (res == null) {
                    row.setNull(columnSeq, Types.LONGVARBINARY);
                } else {
                    row.setBytes(columnSeq, res);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<PreparedStatement> getNullSetter() {
        return new WrapperSetter<PreparedStatement>() {
            @Override
            public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setNull(columnSeq, Types.NULL);
            }
        };
    }
}
