package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.parquet.ParquetUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetHerculesDataTypeConverter;
import lombok.NonNull;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class ParquetHerculesOutputWrapperManager extends ParquetOutputWrapperManager {

    public ParquetHerculesOutputWrapperManager() {
        super(ParquetHerculesDataTypeConverter.getInstance());
    }

    @Override
    protected WrapperSetter<Group> getByteSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res != null) {
                    row.add(columnName, res.byteValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getShortSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res != null) {
                    row.add(columnName, res.shortValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getLonglongSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger res = wrapper.asBigInteger();
                if (res != null) {
                    row.add(columnName, ParquetUtils.bigIntegerToLonglong(res));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getDecimalSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Type columnType = row.getType().getType(columnName);
                // parquet类型一定是decimal
                LogicalTypeAnnotation.DecimalLogicalTypeAnnotation annotation
                        = (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) columnType.getLogicalTypeAnnotation();
                int precision = annotation.getPrecision();
                int scale = annotation.getScale();
                BigDecimal res = wrapper.asBigDecimal();
                if (res != null) {
                    row.add(columnName, ParquetUtils.decimalToBytes(res, precision, scale));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getDateSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Date res = wrapper.asDate();
                if (res != null) {
                    row.add(columnName, ParquetUtils.dateToInt(res));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getTimeSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Date res = wrapper.asDate();
                if (res != null) {
                    row.add(columnName, ParquetUtils.timeToInt(res));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getDatetimeSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Date res = wrapper.asDate();
                if (res != null) {
                    row.add(columnName, ParquetUtils.datetimeToLong(res));
                }
            }
        };
    }
}
