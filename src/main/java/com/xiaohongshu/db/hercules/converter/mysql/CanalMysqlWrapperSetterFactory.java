package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;

import java.nio.charset.StandardCharsets;

public class CanalMysqlWrapperSetterFactory extends WrapperSetterFactory<CanalEntry.Column.Builder> {
    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getByteSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBigInteger().shortValueExact());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getShortSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBigInteger().intValueExact());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getIntegerSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBigInteger().longValueExact());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getLongSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBigInteger());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getFloatSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(OverflowUtils.numberToFloat(wrapper.asBigDecimal()));
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDoubleSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(OverflowUtils.numberToDouble(wrapper.asBigDecimal()));
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDecimalSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBigDecimal());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getBooleanSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = String.valueOf(wrapper.asBoolean());
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getStringSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDateSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getTimeSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asDate().toString();
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getDatetimeSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = wrapper.asString();
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getBytesSetter() {
        return (wrapper, builder, cf, name, seq) -> {
            String res = new String(wrapper.asBytes(), StandardCharsets.UTF_8);
            builder.setValue(res);
        };
    }

    @Override
    protected WrapperSetter<CanalEntry.Column.Builder> getNullSetter() {
        return null;
    }
}
