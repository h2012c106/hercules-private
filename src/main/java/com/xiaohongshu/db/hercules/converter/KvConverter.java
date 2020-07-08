package com.xiaohongshu.db.hercules.converter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class KvConverter<T> {

    protected DataTypeConverter dataTypeConverter;
    protected WrapperGetterFactory wrapperGetterFactory;
    protected WrapperSetterFactory wrapperSetterFactory;

    public KvConverter(DataTypeConverter dataTypeConverter, WrapperGetterFactory wrapperGetterFactory, WrapperSetterFactory wrapperSetterFactory) {
        this.dataTypeConverter = dataTypeConverter;
        this.wrapperGetterFactory = wrapperGetterFactory;
        this.wrapperSetterFactory = wrapperSetterFactory;
    }

    public abstract byte[] generateValue(HerculesWritable value, GenericOptions options, Map<String, DataType> columnTypeMap, List<String> columnNameList);
    public abstract HerculesWritable generateHerculesWritable(byte[] data, GenericOptions options) throws IOException;

    protected final WrapperGetter<CanalEntry.Column> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }

    protected final WrapperSetter<CanalEntry.Column.Builder> getWrapperSetter(DataType dataType) {
        return wrapperSetterFactory.getWrapperSetter(dataType);
    }
}
