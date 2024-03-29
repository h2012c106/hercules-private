package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import org.apache.parquet.example.data.Group;

import java.math.BigDecimal;

public class ParquetSqoopOutputWrapperManager extends ParquetOutputWrapperManager {

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
                row.add(columnName, value.toPlainString());
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
                row.add(columnName, value.getDate().getTime());
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
                row.add(columnName, value.getDate().getTime());
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
                row.add(columnName, value.getDate().getTime());
            }
        };
    }
}
