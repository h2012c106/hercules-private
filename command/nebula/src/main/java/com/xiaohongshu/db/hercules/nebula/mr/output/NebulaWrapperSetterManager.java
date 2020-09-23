package com.xiaohongshu.db.hercules.nebula.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;
import com.xiaohongshu.db.hercules.nebula.WritingRow;

public class NebulaWrapperSetterManager extends WrapperSetterFactory<WritingRow> {

    private static final String DEFAULT_INT_VALUE = "0";
    private static final String DEFAULT_DOUBLE_VALUE = "0.0";
    private static final String DEFAULT_STRING_VALUE = "\"\"";
    private static final String DEFAULT_BOOL_VALUE = "false";
    private static final String DEFAULT_TIMESTAMP_VALUE = "0";

    public NebulaWrapperSetterManager() {
        super(DataSourceRole.TARGET);
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<WritingRow> getByteSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<WritingRow> getShortSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<WritingRow> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<WritingRow>() {
            @Override
            protected void setNonnullValue(Integer value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, value.toString());
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, DEFAULT_INT_VALUE);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<WritingRow> getLongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<WritingRow> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<WritingRow> getFloatSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<WritingRow> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<WritingRow>() {
            @Override
            protected void setNonnullValue(Double value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, value.toString());
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, DEFAULT_DOUBLE_VALUE);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<WritingRow> getDecimalSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<WritingRow> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<WritingRow>() {
            @Override
            protected void setNonnullValue(Boolean value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, value.toString());
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, DEFAULT_BOOL_VALUE);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<WritingRow> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<WritingRow>() {
            @Override
            protected void setNonnullValue(String value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, DEFAULT_STRING_VALUE);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<WritingRow> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<WritingRow> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<WritingRow> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<WritingRow>() {
            @Override
            protected void setNonnullValue(ExtendedDate value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, "\"" + DateUtils.dateToString(value.getDate(), BaseDataType.DATETIME, DateUtils.getTargetDateFormat()) + "\"");
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putValue(columnName, DEFAULT_TIMESTAMP_VALUE);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<WritingRow> getBytesSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<WritingRow> getNullSetter() {
        return null;
    }
}
