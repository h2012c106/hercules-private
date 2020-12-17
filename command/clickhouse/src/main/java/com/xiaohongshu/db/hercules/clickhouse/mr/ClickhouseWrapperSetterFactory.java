package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSWrapperSetterFactory;
import lombok.NonNull;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.Collections;

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
    protected BaseTypeWrapperSetter.ByteSetter<PreparedStatement> getByteSetter() {
        if (enableNull) {
            return super.getByteSetter();
        } else {
            return new BaseTypeWrapperSetter.ByteSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setByte(columnSeq, DEFAULT_INTEGER);
                }

                @Override
                protected void setNonnullValue(Byte value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setByte(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<PreparedStatement> getShortSetter() {
        if (enableNull) {
            return super.getShortSetter();
        } else {
            return new BaseTypeWrapperSetter.ShortSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setShort(columnSeq, DEFAULT_INTEGER);
                }

                @Override
                protected void setNonnullValue(Short value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setShort(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<PreparedStatement> getIntegerSetter() {
        if (enableNull) {
            return super.getIntegerSetter();
        } else {
            return new BaseTypeWrapperSetter.IntegerSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setInt(columnSeq, DEFAULT_INTEGER);
                }

                @Override
                protected void setNonnullValue(Integer value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setInt(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<PreparedStatement> getLongSetter() {
        if (enableNull) {
            return super.getLongSetter();
        } else {
            return new BaseTypeWrapperSetter.LongSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setLong(columnSeq, DEFAULT_INTEGER);
                }

                @Override
                protected void setNonnullValue(Long value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setLong(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<PreparedStatement> getFloatSetter() {
        if (enableNull) {
            return super.getFloatSetter();
        } else {
            return new BaseTypeWrapperSetter.FloatSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setFloat(columnSeq, DEFAULT_FLOAT);
                }

                @Override
                protected void setNonnullValue(Float value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setFloat(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<PreparedStatement> getDoubleSetter() {
        if (enableNull) {
            return super.getDoubleSetter();
        } else {
            return new BaseTypeWrapperSetter.DoubleSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setDouble(columnSeq, DEFAULT_FLOAT);
                }

                @Override
                protected void setNonnullValue(Double value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setDouble(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<PreparedStatement> getDecimalSetter() {
        if (enableNull) {
            return super.getDecimalSetter();
        } else {
            return new BaseTypeWrapperSetter.DecimalSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setBigDecimal(columnSeq, DEFAULT_DECIMAL);
                }

                @Override
                protected void setNonnullValue(BigDecimal value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setBigDecimal(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<PreparedStatement> getBooleanSetter() {
        // clickhouse没有boolean
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<PreparedStatement> getStringSetter() {
        if (enableNull) {
            return super.getStringSetter();
        } else {
            return new BaseTypeWrapperSetter.StringSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setString(columnSeq, DEFAULT_STRING);
                }

                @Override
                protected void setNonnullValue(String value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setString(columnSeq, value);
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<PreparedStatement> getDateSetter() {
        if (enableNull) {
            return super.getDateSetter();
        } else {
            return new BaseTypeWrapperSetter.DateSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setDate(columnSeq, new java.sql.Date(DEFAULT_TIMESTAMP));

                }

                @Override
                protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setDate(columnSeq, new java.sql.Date(value.getDate().getTime()));
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<PreparedStatement> getTimeSetter() {
        if (enableNull) {
            return super.getTimeSetter();
        } else {
            return new BaseTypeWrapperSetter.TimeSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setTime(columnSeq, new java.sql.Time(DEFAULT_TIMESTAMP));
                }

                @Override
                protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setTime(columnSeq, new java.sql.Time(value.getDate().getTime()));
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<PreparedStatement> getDatetimeSetter() {
        if (enableNull) {
            return super.getDatetimeSetter();
        } else {
            return new BaseTypeWrapperSetter.DatetimeSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setTimestamp(columnSeq, new java.sql.Timestamp(DEFAULT_TIMESTAMP));
                }

                @Override
                protected void setNonnullValue(ExtendedDate value, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    if (value.isZero()) {
                        row.setString(columnSeq, DateUtils.ZERO_DATE);
                    } else {
                        row.setTimestamp(columnSeq, new java.sql.Timestamp(value.getDate().getTime()));
                    }
                }
            };
        }
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<PreparedStatement> getBytesSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<PreparedStatement> getListSetter() {
        if (enableNull) {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setObject(columnSeq, null);
                }

                @Override
                protected void setNonnull(@NonNull BaseWrapper<?> wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    ListWrapper list;
                    if (wrapper instanceof ListWrapper) {
                        list = (ListWrapper) wrapper;
                    } else {
                        list = new ListWrapper(1);
                        list.add(wrapper);
                    }
                    // 参考https://github.com/ClickHouse/clickhouse-jdbc/blob/master/src/test/java/ru/yandex/clickhouse/integration/ArrayTest.java#L125
                    row.setObject(columnSeq, list.asDefault());
                }
            };
        } else {
            return new WrapperSetter<PreparedStatement>() {
                @Override
                protected void setNull(PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    row.setObject(columnSeq, Collections.emptyList());
                }

                @Override
                protected void setNonnull(@NonNull BaseWrapper<?> wrapper, PreparedStatement row, String rowName, String columnName, int columnSeq) throws Exception {
                    ListWrapper list;
                    if (wrapper instanceof ListWrapper) {
                        list = (ListWrapper) wrapper;
                    } else {
                        list = new ListWrapper(1);
                        list.add(wrapper);
                    }
                    // 参考https://github.com/ClickHouse/clickhouse-jdbc/blob/master/src/test/java/ru/yandex/clickhouse/integration/ArrayTest.java#L125
                    row.setObject(columnSeq, list.asDefault());
                }
            };
        }
    }
}
