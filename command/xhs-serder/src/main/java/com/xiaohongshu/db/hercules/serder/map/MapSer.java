package com.xiaohongshu.db.hercules.serder.map;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serder.KVSer;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

import java.io.IOException;

public class MapSer extends KVSer<Void> {

    public MapSer() {
        super(new WrapperSetterFactory<Void>(DataSourceRole.SER) {

            @Override
            protected BaseTypeWrapperSetter.ByteSetter<Void> getByteSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.ShortSetter<Void> getShortSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.IntegerSetter<Void> getIntegerSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.LongSetter<Void> getLongSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.LonglongSetter<Void> getLonglongSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.FloatSetter<Void> getFloatSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.DoubleSetter<Void> getDoubleSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.DecimalSetter<Void> getDecimalSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.BooleanSetter<Void> getBooleanSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.StringSetter<Void> getStringSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.DateSetter<Void> getDateSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.TimeSetter<Void> getTimeSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.DatetimeSetter<Void> getDatetimeSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.BytesSetter<Void> getBytesSetter() {
                return null;
            }

            @Override
            protected BaseTypeWrapperSetter.NullSetter<Void> getNullSetter() {
                return null;
            }
        });
    }

    @Override
    protected BaseWrapper<?> writeValue(HerculesWritable in) throws IOException, InterruptedException {
        return in.getRow();
    }
}
