package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DoubleWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;

public class ParquetHerculesInputWrapperManager extends ParquetInputWrapperManager {

    private final boolean emptyAsNull;

    public ParquetHerculesInputWrapperManager(boolean emptyAsNull) {
        super(ParquetHerculesDataTypeConverter.getInstance());
        this.emptyAsNull = emptyAsNull;
    }

    @Override
    protected boolean emptyAsNull() {
        return emptyAsNull;
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
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return IntegerWrapper.get(row.isEmpty()
                        ? null
                        : ParquetUtils.longlongToBigInteger(row.getGroup().getInt96(columnName, row.getValueSeq())));
            }
        };
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
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return DateWrapper.get(row.isEmpty()
                                ? null
                                : ParquetUtils.intToTime(row.getGroup().getInteger(columnName, row.getValueSeq())),
                        BaseDataType.TIME);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDatetimeGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return DateWrapper.get(row.isEmpty()
                                ? null
                                : ParquetUtils.longToDatetime(row.getGroup().getLong(columnName, row.getValueSeq())),
                        BaseDataType.DATETIME);
            }
        };
    }
}
