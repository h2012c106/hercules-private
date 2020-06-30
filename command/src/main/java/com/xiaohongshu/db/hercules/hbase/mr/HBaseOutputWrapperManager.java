package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOutputWrapperManager extends WrapperSetterFactory<Put> {
    @Override
    protected WrapperSetter<Put> getByteSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Put> getShortSetter() {
        return (wrapper, put, columnFamily, name, seq) -> {
            Short res = wrapper.asShort();
            put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
        };
    }

    @Override
    protected WrapperSetter<Put> getIntegerSetter() {
        return (wrapper, put, columnFamily, name, seq) -> {
            Integer res = wrapper.asInteger();
            put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
        };
    }

    @Override
    protected WrapperSetter<Put> getLongSetter() {
        return (wrapper, put, columnFamily, name, seq) -> {
            Long res = wrapper.asLong();
            put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(res));
        };
    }

    @Override
    protected WrapperSetter<Put> getLonglongSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Put> getFloatSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asFloat()));
    }

    @Override
    protected WrapperSetter<Put> getDoubleSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asDouble()));
    }

    @Override
    protected WrapperSetter<Put> getDecimalSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asBigDecimal()));
    }

    @Override
    protected WrapperSetter<Put> getBooleanSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asBoolean()));
    }

    @Override
    protected WrapperSetter<Put> getStringSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), Bytes.toBytes(wrapper.asString()));
    }

    @Override
    protected WrapperSetter<Put> getDateSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Put> getTimeSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Put> getDatetimeSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Put> getBytesSetter() {
        return (wrapper, put, columnFamily, name, seq) ->
                put.addColumn(columnFamily.getBytes(), name.getBytes(), wrapper.asBytes());
    }

    @Override
    protected WrapperSetter<Put> getNullSetter() {
        return null;
    }
}
