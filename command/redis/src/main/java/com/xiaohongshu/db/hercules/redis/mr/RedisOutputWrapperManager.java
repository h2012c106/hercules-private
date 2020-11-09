package com.xiaohongshu.db.hercules.redis.mr;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.redis.RedisKV;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashMap;
import java.util.Map;

import static com.xiaohongshu.db.hercules.redis.RedisKV.VALUE_SEQ;

/**
 * Created by jamesqq on 2020/8/17.
 */
public class RedisOutputWrapperManager extends WrapperSetterFactory<RedisKV> {

    public RedisOutputWrapperManager() {
        super(DataSourceRole.TARGET);
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<RedisKV> getByteSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<RedisKV> getShortSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<RedisKV> getIntegerSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<RedisKV> getLongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<RedisKV> getLonglongSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<RedisKV> getFloatSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<RedisKV> getDoubleSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<RedisKV> getDecimalSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<RedisKV> getBooleanSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<RedisKV> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<RedisKV>() {
            @Override
            protected void setNull(RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(RedisKV.RedisKVValue.initialize(getType(), null), columnSeq);
            }

            @Override
            protected void setNonnullValue(String value, RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(RedisKV.RedisKVValue.initialize(getType(), value), columnSeq);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<RedisKV> getDateSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<RedisKV> getTimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<RedisKV> getDatetimeSetter() {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<RedisKV> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<RedisKV>() {
            @Override
            protected void setNull(RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(RedisKV.RedisKVValue.initialize(getType(), null), columnSeq);
            }

            @Override
            protected void setNonnullValue(byte[] value, RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(RedisKV.RedisKVValue.initialize(getType(), value), columnSeq);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<RedisKV> getNullSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<RedisKV> getMapSetter() {
        return new MapWrapperSetter<RedisKV>() {
            @Override
            protected void setNull(RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                row.set(RedisKV.RedisKVValue.initialize(getType(), null), columnSeq);
            }

            @Override
            protected void setNonnull(BaseWrapper<?> value, RedisKV row, String rowName, String columnName, int columnSeq) throws Exception {
                MapWrapper mapWrapper = (MapWrapper) value;
                Map<byte[], byte[]> map = new HashMap<>();
                for (Map.Entry<String, BaseWrapper<?>> entry : mapWrapper.entrySet()) {
                    RedisKV tmp = new RedisKV();
                    //因为redis底层存的都是bytes，即使是string实际也是按照bytes
                    getWrapperSetter(BaseDataType.BYTES).set(entry.getValue(), tmp, null, null, VALUE_SEQ);
                map.put(SafeEncoder.encode(entry.getKey()), (byte[])(tmp.getValue().getValue()));
            }
                row.set(RedisKV.RedisKVValue.initialize(getType(), map), columnSeq);
            }
        };
    }


}