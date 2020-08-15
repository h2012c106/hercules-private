package com.xiaohongshu.db.hercules.kafka.mr;

import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.kafka.KafkaKV;

public class KafkaOutputWrapperManager extends WrapperSetterFactory<KafkaKV> {

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<KafkaKV> getByteSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<KafkaKV> getShortSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<KafkaKV> getIntegerSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<KafkaKV> getLongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<KafkaKV> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<KafkaKV> getFloatSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<KafkaKV> getDoubleSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<KafkaKV> getDecimalSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<KafkaKV> getBooleanSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<KafkaKV> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<KafkaKV>() {
            @Override
            protected void setNull(KafkaKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(KafkaKV.KafkaKVValue.initialize(getType(), null), columnSeq);
            }

            @Override
            protected void setNonnullValue(String value, KafkaKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(KafkaKV.KafkaKVValue.initialize(getType(), value), columnSeq);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<KafkaKV> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<KafkaKV> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<KafkaKV> getDatetimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<KafkaKV> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<KafkaKV>() {
            @Override
            protected void setNull(KafkaKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(KafkaKV.KafkaKVValue.initialize(getType(), null), columnSeq);
            }

            @Override
            protected void setNonnullValue(byte[] value, KafkaKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(KafkaKV.KafkaKVValue.initialize(getType(), value), columnSeq);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<KafkaKV> getNullSetter() {
        return null;
    }
}
