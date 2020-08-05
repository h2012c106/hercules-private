package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DateWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DoubleWrapper;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSqoopDataTypeConverter;

import java.math.BigDecimal;

public class ParquetSqoopInputWrapperManager extends ParquetInputWrapperManager {

    public ParquetSqoopInputWrapperManager() {
        super(ParquetSqoopDataTypeConverter.getInstance());
    }

    @Override
    protected boolean emptyAsNull() {
        return true;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getByteGetter() {
        return getIntegerGetter();
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getShortGetter() {
        return getIntegerGetter();
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
                return DoubleWrapper.get(row.isEmpty()
                        ? null
                        : new BigDecimal(row.getGroup().getString(columnName, row.getValueSeq())));
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
                                : row.getGroup().getLong(columnName, row.getValueSeq()),
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
                                : row.getGroup().getLong(columnName, row.getValueSeq()),
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
                                : row.getGroup().getLong(columnName, row.getValueSeq()),
                        BaseDataType.DATETIME);
            }
        };
    }
}
