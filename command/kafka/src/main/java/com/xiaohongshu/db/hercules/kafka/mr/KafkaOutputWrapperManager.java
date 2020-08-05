package com.xiaohongshu.db.hercules.kafka.mr;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;

public class KafkaOutputWrapperManager extends WrapperSetterFactory<CanalEntry.Entry> {

    @Override
    protected WrapperSetter<CanalEntry.Entry> getByteSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getShortSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getIntegerSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getLongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getFloatSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getDoubleSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getDecimalSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getBooleanSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getStringSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getDateSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getTimeSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getDatetimeSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getBytesSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<CanalEntry.Entry> getNullSetter() {
        return null;
    }
}
