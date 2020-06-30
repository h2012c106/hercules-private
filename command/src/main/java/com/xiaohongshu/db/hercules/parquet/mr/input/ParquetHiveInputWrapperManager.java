package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
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
    protected WrapperGetter<GroupWithSchemaInfo> getByteGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return IntegerWrapper.get(row.isEmpty()
                        ? null
                        : OverflowUtils.numberToByte(row.getGroup().getInteger(columnName, row.getValueSeq())));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getShortGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return IntegerWrapper.get(row.isEmpty()
                        ? null
                        : OverflowUtils.numberToShort(row.getGroup().getInteger(columnName, row.getValueSeq())));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getLonglongGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDecimalGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                if (row.isEmpty()) {
                    return DoubleWrapper.get((BigDecimal) null);
                } else {
                    Type columnType = row.getGroup().getType().getType(columnName);
                    // parquet类型一定是decimal
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation annotation
                            = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) columnType.getLogicalTypeAnnotation();
                    int scale = annotation.getScale();
                    return DoubleWrapper.get(ParquetUtils.bytesToDecimal(row.getGroup().getBinary(columnName, row.getValueSeq()), scale));
                }
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDateGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return DateWrapper.get(row.isEmpty()
                                ? null
                                : ParquetUtils.intToDate(row.getGroup().getInteger(columnName, row.getValueSeq())),
                        BaseDataType.DATE);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getTimeGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDatetimeGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return DateWrapper.get(row.isEmpty()
                                ? null
                                : ParquetUtils.bytesToDatetime(row.getGroup().getInt96(columnName, row.getValueSeq())),
                        BaseDataType.DATETIME);
            }
        };
    }
}
