package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DoubleWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

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
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                byte value = OverflowUtils.numberToByte(row.getGroup().getInteger(columnName, row.getValueSeq()));
                return new IntegerWrapper(value);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getShortGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                short value = OverflowUtils.numberToShort(row.getGroup().getInteger(columnName, row.getValueSeq()));
                return new IntegerWrapper(value);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getLonglongGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                return new IntegerWrapper(ParquetUtils.longlongToBigInteger(row.getGroup().getInt96(columnName, row.getValueSeq())));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDecimalGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                Type columnType = row.getGroup().getType().getType(columnName);
                // parquet类型一定是decimal
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation annotation
                        = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) columnType.getLogicalTypeAnnotation();
                int scale = annotation.getScale();
                return new DoubleWrapper(ParquetUtils.bytesToDecimal(row.getGroup().getBinary(columnName, row.getValueSeq()), scale));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDateGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                return new DateWrapper(ParquetUtils.intToDate(row.getGroup().getInteger(columnName, row.getValueSeq())), DataType.DATE);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getTimeGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                return new DateWrapper(ParquetUtils.intToTime(row.getGroup().getInteger(columnName, row.getValueSeq())), DataType.TIME);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDatetimeGetter() {
        return new ParquetWrapperGetter() {
            @Override
            protected BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                return new DateWrapper(ParquetUtils.longToDatetime(row.getGroup().getLong(columnName, row.getValueSeq())), DataType.DATETIME);
            }
        };
    }
}
