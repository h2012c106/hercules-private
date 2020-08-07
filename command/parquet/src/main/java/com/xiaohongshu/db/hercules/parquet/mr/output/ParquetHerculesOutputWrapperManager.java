package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ParquetHerculesOutputWrapperManager extends ParquetOutputWrapperManager {

    public ParquetHerculesOutputWrapperManager() {
        super(ParquetHerculesDataTypeConverter.getInstance());
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<Group> getByteSetter() {
        return new BaseTypeWrapperSetter.ByteSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Byte value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<Group> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Short value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<Group> getLonglongSetter() {
        return new BaseTypeWrapperSetter.LonglongSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(BigInteger value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, ParquetUtils.bigIntegerToLonglong(value));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<Group> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(BigDecimal value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Type columnType = row.getType().getType(columnName);
                // parquet类型一定是decimal
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation annotation
                        = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) columnType.getLogicalTypeAnnotation();
                int precision = annotation.getPrecision();
                int scale = annotation.getScale();
                row.add(columnName, ParquetUtils.decimalToBytes(value, precision, scale));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<Group> getDateSetter() {
        return new BaseTypeWrapperSetter.DateSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, ParquetUtils.dateToInt(value.getDate()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<Group> getTimeSetter() {
        return new BaseTypeWrapperSetter.TimeSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, ParquetUtils.timeToInt(value.getDate()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<Group> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, ParquetUtils.datetimeToLong(value.getDate()));
            }
        };
    }
}
