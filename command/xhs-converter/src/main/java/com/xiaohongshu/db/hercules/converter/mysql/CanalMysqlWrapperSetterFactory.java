package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class CanalMysqlWrapperSetterFactory extends WrapperSetterFactory<CanalEntry.Column.Builder> {
    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getByteSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res.shortValueExact());
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getShortSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res.intValueExact());
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getIntegerSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res.longValueExact());
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getLongSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigInteger res = wrapper.asBigInteger();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res);
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getFloatSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(OverflowUtils.numberToFloat(res));
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDoubleSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(OverflowUtils.numberToDouble(res));
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDecimalSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            BigDecimal res = wrapper.asBigDecimal();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res);
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getBooleanSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            Boolean res = wrapper.asBoolean();
            if (res == null){
                builder.setIsNull(true);
            } else {
                String val = String.valueOf(res);
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getStringSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            if (res == null){
                builder.setIsNull(true);
            } else {
                builder.setValue(res);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDateSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            if (res == null){
                builder.setIsNull(true);
            } else {
                builder.setValue(res);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getTimeSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            Date res = wrapper.asDate();
            if (res == null) {
                builder.setValue("");
            } else {
                builder.setValue(new java.sql.Time(res.getTime()).toString());
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDatetimeSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            if (res == null) {
                builder.setValue("");
            } else {
                builder.setValue(res);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getBytesSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            byte[] res = wrapper.asBytes();
            if (res == null) {
                builder.setValue("");
            } else {
                String val = new String(res, StandardCharsets.UTF_8);
                builder.setValue(val);
            }
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getNullSetter() {
        return null;
    }
}
