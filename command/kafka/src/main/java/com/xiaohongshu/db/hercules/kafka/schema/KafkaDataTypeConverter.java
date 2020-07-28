package com.xiaohongshu.db.hercules.kafka.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;

import java.util.Map;

public class KafkaDataTypeConverter implements DataTypeConverter<Integer, byte[]> {

    @Override
    public DataType convertElementType(Integer standard) {
        return null;
    }

    @Override
    public Map<String, DataType> convertRowType(byte[] line) {
        return null;
    }
}
