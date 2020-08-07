package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DoubleWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHiveDataTypeConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;

public class ParquetHiveInputWrapperManager extends ParquetInputWrapperManager {

    public ParquetHiveInputWrapperManager() {
        super(ParquetHiveDataTypeConverter.getInstance());
    }

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
                Type columnType = row.getGroup().getType().getType(columnName);
                // parquet类型一定是decimal
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation annotation
                        = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) columnType.getLogicalTypeAnnotation();
                int scale = annotation.getScale();
                return ParquetUtils.bytesToDecimal(row.getGroup().getBinary(columnName, row.getValueSeq()), scale);
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
                return new ExtendedDate(ParquetUtils.intToDate(row.getGroup().getInteger(columnName, row.getValueSeq())));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.TimeGetter<GroupWithSchemaInfo> getTimeGetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperGetter.DatetimeGetter<GroupWithSchemaInfo> getDatetimeGetter() {
        return new BaseTypeWrapperGetter.DatetimeGetter<GroupWithSchemaInfo>() {
            @Override
            protected ExtendedDate getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return new ExtendedDate(ParquetUtils.bytesToDatetime(row.getGroup().getInt96(columnName, row.getValueSeq())));
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }
}
