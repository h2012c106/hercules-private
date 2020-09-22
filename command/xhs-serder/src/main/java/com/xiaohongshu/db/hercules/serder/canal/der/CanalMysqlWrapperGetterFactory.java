package com.xiaohongshu.db.hercules.serder.canal.der;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import org.apache.commons.beanutils.ConvertUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;

/**
 * 参阅{@see <a href="https://github.com/alibaba/otter/blob/master/node/etl/src/main/java/com/alibaba/otter/node/etl/common/db/utils/SqlUtils.java#L130">otter逻辑</a>}
 */
public class CanalMysqlWrapperGetterFactory extends WrapperGetterFactory<CanalEntry.Column> {

    @Override
    public DataSourceRole getRole() {
        return DataSourceRole.DER;
    }

    @Override
    protected BaseTypeWrapperGetter.ByteGetter<CanalEntry.Column> getByteGetter() {
        return new BaseTypeWrapperGetter.ByteGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Byte getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Byte) ConvertUtils.convert(row.getValue().trim(), Byte.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.ShortGetter<CanalEntry.Column> getShortGetter() {
        return new BaseTypeWrapperGetter.ShortGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Short getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Short) ConvertUtils.convert(row.getValue().trim(), Short.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.IntegerGetter<CanalEntry.Column> getIntegerGetter() {
        return new BaseTypeWrapperGetter.IntegerGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Integer getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Integer) ConvertUtils.convert(row.getValue().trim(), Integer.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LongGetter<CanalEntry.Column> getLongGetter() {
        return new BaseTypeWrapperGetter.LongGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Long getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Long) ConvertUtils.convert(row.getValue().trim(), Long.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LonglongGetter<CanalEntry.Column> getLonglongGetter() {
        return new BaseTypeWrapperGetter.LonglongGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected BigInteger getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (BigInteger) ConvertUtils.convert(row.getValue().trim(), BigInteger.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.FloatGetter<CanalEntry.Column> getFloatGetter() {
        return new BaseTypeWrapperGetter.FloatGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Float getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Float) ConvertUtils.convert(row.getValue().trim(), Float.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DoubleGetter<CanalEntry.Column> getDoubleGetter() {
        return new BaseTypeWrapperGetter.DoubleGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Double getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Double) ConvertUtils.convert(row.getValue().trim(), Double.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DecimalGetter<CanalEntry.Column> getDecimalGetter() {
        return new BaseTypeWrapperGetter.DecimalGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected BigDecimal getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (BigDecimal) ConvertUtils.convert(row.getValue().trim(), BigDecimal.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BooleanGetter<CanalEntry.Column> getBooleanGetter() {
        return new BaseTypeWrapperGetter.BooleanGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected Boolean getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return (Boolean) ConvertUtils.convert(row.getValue(), Boolean.class);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.StringGetter<CanalEntry.Column> getStringGetter() {
        return new BaseTypeWrapperGetter.StringGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected String getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getValue();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DateGetter<CanalEntry.Column> getDateGetter() {
        return new BaseTypeWrapperGetter.DateGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected ExtendedDate getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize((Date) ConvertUtils.convert(row.getValue(), java.sql.Date.class));
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<CanalEntry.Column> getTimeGetter() {
        return new BaseTypeWrapperGetter.TimeGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected ExtendedDate getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize((Date) ConvertUtils.convert(row.getValue(), java.sql.Time.class));
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<CanalEntry.Column> getDatetimeGetter() {
        return new BaseTypeWrapperGetter.DatetimeGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected ExtendedDate getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                if (row.getValue().equals(DateUtils.ZERO_DATE)) {
                    return ExtendedDate.ZERO_INSTANCE;
                } else {
                    return ExtendedDate.initialize((Date) ConvertUtils.convert(row.getValue(), java.sql.Timestamp.class));
                }
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BytesGetter<CanalEntry.Column> getBytesGetter() {
        return new BaseTypeWrapperGetter.BytesGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getIsNull();
            }

            @Override
            protected byte[] getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getValue().getBytes(StandardCharsets.ISO_8859_1);
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.NullGetter<CanalEntry.Column> getNullGetter() {
        return new BaseTypeWrapperGetter.NullGetter<CanalEntry.Column>() {
            @Override
            protected boolean isNull(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return true;
            }

            @Override
            protected Void getNonnullValue(CanalEntry.Column row, String rowName, String columnName, int columnSeq) throws Exception {
                return null;
            }
        };
    }
}