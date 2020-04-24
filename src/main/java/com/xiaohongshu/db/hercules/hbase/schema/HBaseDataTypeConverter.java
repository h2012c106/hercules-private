package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

public class HBaseDataTypeConverter  implements DataTypeConverter<Integer, Result> {
    @Override
    public DataType convertElementType(Integer standard) {
        return null;
    }

    @Override
    public Map<String, DataType> convertRowType(Result line) {
        return null;
    }
}
