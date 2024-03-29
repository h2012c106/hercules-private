package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;

public class ParquetSqoopInputWrapperManager extends ParquetInputWrapperManager {

    @Override
    protected boolean emptyAsNull() {
        return true;
    }

    @Override
    protected BaseTypeWrapperGetter.ByteGetter<GroupWithSchemaInfo> getByteGetter() {
        return new BaseTypeWrapperGetter.ByteGetter<GroupWithSchemaInfo>() {
            @Override
            protected Byte getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return OverflowUtils.numberToByte(row.getGroup().getInteger(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.ShortGetter<GroupWithSchemaInfo> getShortGetter() {
        return new BaseTypeWrapperGetter.ShortGetter<GroupWithSchemaInfo>() {
            @Override
            protected Short getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return OverflowUtils.numberToShort(row.getGroup().getInteger(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LonglongGetter<GroupWithSchemaInfo> getLonglongGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.DecimalGetter<GroupWithSchemaInfo> getDecimalGetter() {
        return new BaseTypeWrapperGetter.DecimalGetter<GroupWithSchemaInfo>() {
            @Override
            protected BigDecimal getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return new BigDecimal(row.getGroup().getString(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DateGetter<GroupWithSchemaInfo> getDateGetter() {
        return new BaseTypeWrapperGetter.DateGetter<GroupWithSchemaInfo>() {
            @Override
            protected ExtendedDate getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.getGroup().getLong(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<GroupWithSchemaInfo> getTimeGetter() {
        return new BaseTypeWrapperGetter.TimeGetter<GroupWithSchemaInfo>() {
            @Override
            protected ExtendedDate getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.getGroup().getLong(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<GroupWithSchemaInfo> getDatetimeGetter() {
        return new BaseTypeWrapperGetter.DatetimeGetter<GroupWithSchemaInfo>() {
            @Override
            protected ExtendedDate getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return ExtendedDate.initialize(row.getGroup().getLong(columnName, row.getValueSeq()));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }
}
