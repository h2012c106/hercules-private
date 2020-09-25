package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;

import static com.xiaohongshu.db.hercules.parquet.option.ParquetOptionsConf.TS_SKIP_CONVERSION;

public class ParquetHiveOutputWrapperManager extends ParquetOutputWrapperManager implements InjectedClass {

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    private boolean skipConversion;

    @Override
    public void afterInject() {
        skipConversion = targetOptions.getBoolean(TS_SKIP_CONVERSION, false);
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
        return null;
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
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<Group> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, ParquetUtils.datetimeToBytes(value.getDate(), skipConversion));
            }
        };
    }
}
