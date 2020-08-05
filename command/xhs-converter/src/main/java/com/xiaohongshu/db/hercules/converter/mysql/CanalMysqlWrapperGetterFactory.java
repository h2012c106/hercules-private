package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import com.xiaohongshu.db.hercules.core.utils.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public class CanalMysqlWrapperGetterFactory extends WrapperGetterFactory<CanalEntry.Column> {
    @Override
    protected WrapperGetter<CanalEntry.Column> getByteGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return IntegerWrapper.get(Short.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getShortGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return IntegerWrapper.get(Integer.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getIntegerGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return IntegerWrapper.get(Long.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getLongGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return IntegerWrapper.get(new BigInteger(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getLonglongGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getFloatGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DoubleWrapper.get(Float.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getDoubleGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DoubleWrapper.get(Double.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getDecimalGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DoubleWrapper.get(new BigDecimal(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getBooleanGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return BooleanWrapper.get(Boolean.valueOf(res));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getStringGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return StringWrapper.get(res);
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getDateGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DateWrapper.get(DateUtils.stringToDate(res, DateUtils.getSourceDateFormat()), BaseDataType.DATE);
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getTimeGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DateWrapper.get(new java.sql.Time(DateUtils.stringToDate(res, DateUtils.getSourceDateFormat()).getTime()), BaseDataType.DATE);
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getDatetimeGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            return DateWrapper.get(res, BaseDataType.DATE);
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getBytesGetter() {
        return (row, rowName, columnName, columnSeq) -> {
            String res = row.getValue();
            // TODO 确定合适的decoder
            return BytesWrapper.get(res.getBytes(StandardCharsets.UTF_8));
        };
    }

    @Override
    protected WrapperGetter<CanalEntry.Column> getNullGetter() {
        return (row, rowName, columnName, columnSeq) -> NullWrapper.INSTANCE;
    }
}