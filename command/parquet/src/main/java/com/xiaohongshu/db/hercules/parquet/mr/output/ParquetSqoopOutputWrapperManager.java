package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetSqoopDataTypeConverter;
import lombok.NonNull;
import org.apache.parquet.example.data.Group;

import java.math.BigDecimal;
import java.util.Date;

public class ParquetSqoopOutputWrapperManager extends ParquetOutputWrapperManager {

    public ParquetSqoopOutputWrapperManager() {
        super(ParquetSqoopDataTypeConverter.getInstance());
    }

    @Override
    protected WrapperSetter<Group> getByteSetter() {
        return getIntegerSetter();
    }

    @Override
    protected WrapperSetter<Group> getShortSetter() {
        return getIntegerSetter();
    }

    @Override
    protected WrapperSetter<Group> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Group> getDecimalSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal value = wrapper.asBigDecimal();
                if (value != null) {
                    row.add(columnName, value.toPlainString());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getDateSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Date value = wrapper.asDate();
                if (value != null) {
                    row.add(columnName, value.getTime());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getTimeSetter() {
        return getDateSetter();
    }

    @Override
    protected WrapperSetter<Group> getDatetimeSetter() {
        return getDateSetter();
    }
}
