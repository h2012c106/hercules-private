package com.xiaohongshu.db.hercules.serder.canal.ser;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * 参阅{@see <a href="https://github.com/alibaba/canal/blob/master/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L731">canal逻辑</a>}
 */
public class CanalMysqlWrapperSetterFactory extends WrapperSetterFactory<CanalEntry.Column.Builder> {

    @Override
    public DataSourceRole getRole() {
        return DataSourceRole.SER;
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<CanalEntry.Column.Builder> getByteSetter() {
        return new BaseTypeWrapperSetter.ByteSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Byte value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<CanalEntry.Column.Builder> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Short value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<CanalEntry.Column.Builder> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Integer value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<CanalEntry.Column.Builder> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Long value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<CanalEntry.Column.Builder> getLonglongSetter() {
        return new BaseTypeWrapperSetter.LonglongSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(BigInteger value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(value.toString());
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<CanalEntry.Column.Builder> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Float value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<CanalEntry.Column.Builder> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Double value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<CanalEntry.Column.Builder> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(BigDecimal value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(value.toPlainString());
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<CanalEntry.Column.Builder> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Boolean value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(String.valueOf(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<CanalEntry.Column.Builder> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(String value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<CanalEntry.Column.Builder> getDateSetter() {
        return new BaseTypeWrapperSetter.DateSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(new java.sql.Date(value.getDate().getTime()).toString());
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<CanalEntry.Column.Builder> getTimeSetter() {
        return new BaseTypeWrapperSetter.TimeSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(new java.sql.Time(value.getDate().getTime()).toString());
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<CanalEntry.Column.Builder> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                if (value.isZero()) {
                    row.setValue(DateUtils.ZERO_DATE);
                } else {
                    row.setValue(new java.sql.Timestamp(value.getDate().getTime()).toString());
                }
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<CanalEntry.Column.Builder> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(byte[] value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setValue(new String(value, StandardCharsets.ISO_8859_1));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<CanalEntry.Column.Builder> getNullSetter() {
        return new BaseTypeWrapperSetter.NullSetter<CanalEntry.Column.Builder>() {
            @Override
            protected void setNull(CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }

            @Override
            protected void setNonnullValue(Void value, CanalEntry.Column.Builder row, String rowName, String columnName, int columnSeq) throws Exception {
                row.setIsNull(true);
            }
        };
    }
}
