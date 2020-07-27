package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSWrapperSetterFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.util.Date;

public class ClickhouseWrapperSetterFactory extends RDBMSWrapperSetterFactory {

    private final boolean enableNull;

    private static final byte DEFAULT_INTEGER = 0;
    private static final float DEFAULT_FLOAT = 0.0f;
    private static final BigDecimal DEFAULT_DECIMAL = BigDecimal.ZERO;
    private static final String DEFAULT_STRING = "";
    private static final long DEFAULT_TIMESTAMP = 0;

    public ClickhouseWrapperSetterFactory(boolean enableNull) {
        this.enableNull = enableNull;
    }

    @Override
    protected WrapperSetter<PreparedStatement> getByteSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    row.setByte(columnSeq, res == null ? DEFAULT_INTEGER : res.byteValueExact());
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getShortSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    row.setShort(columnSeq, res == null ? DEFAULT_INTEGER : res.shortValueExact());
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getIntegerSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    row.setInt(columnSeq, res == null ? DEFAULT_INTEGER : res.intValueExact());
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getLongSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigInteger res = wrapper.asBigInteger();
                    row.setLong(columnSeq, res == null ? DEFAULT_INTEGER : res.longValueExact());
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getFloatSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    row.setFloat(columnSeq, res == null ? DEFAULT_FLOAT : OverflowUtils.numberToFloat(res));
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDoubleSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    row.setDouble(columnSeq, res == null ? DEFAULT_FLOAT : OverflowUtils.numberToDouble(res));
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDecimalSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    BigDecimal res = wrapper.asBigDecimal();
                    row.setBigDecimal(columnSeq, res == null ? DEFAULT_DECIMAL : res);
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBooleanSetter() {
        // clickhouse没有boolean
        return null;
    }

    @Override
    protected WrapperSetter<PreparedStatement> getStringSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    String res = wrapper.asString();
                    row.setString(columnSeq, res == null ? DEFAULT_STRING : res);
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDateSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    Date res = wrapper.asDate();
                    row.setDate(columnSeq, res == null ? new java.sql.Date(DEFAULT_TIMESTAMP) : new java.sql.Date(res.getTime()));
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getTimeSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    Date res = wrapper.asDate();
                    row.setTime(columnSeq, res == null ? new java.sql.Time(DEFAULT_TIMESTAMP) : new java.sql.Time(res.getTime()));
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getDatetimeSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                public void set(BaseWrapper wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    String res = wrapper.asString();
                    if (res == null) {
                        row.setTimestamp(columnSeq, new java.sql.Timestamp(DEFAULT_TIMESTAMP));
                    } else {
                        row.setString(columnSeq, res);
                    }
                }
            };
        }
    }

    @Override
    protected WrapperSetter<PreparedStatement> getBytesSetter() {
        return null;
    }
}
