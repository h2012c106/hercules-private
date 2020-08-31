package com.xiaohongshu.db.hercules.redis.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;

import java.util.Map;


public class RedisDataTypeConverter implements DataTypeConverter<Integer, Map> {

    @Override
    public DataType convertElementType(Integer standard) {
        return null;
    }

    @Override
    public Map<String, DataType> convertRowType(Map line) {
        return null;
    }

}
