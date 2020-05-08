package com.xiaohongshu.db.hercules.hbase.schema;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import lombok.NonNull;
import org.apache.hadoop.hbase.client.Result;

import java.util.Map;
import java.util.stream.Collectors;

public class HBaseDataTypeConverter  implements DataTypeConverter<Integer, Result> {
    @Override
    public DataType convertElementType(Integer standard) {
        return null;
    }

    @Override
    public Map<String, DataType> convertRowType(Result line) {
        return null;
    }

    public static Map<String, HBaseDataType> convert(@NonNull JSONObject jsonObject) {
        return jsonObject.getInnerMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> HBaseDataType.valueOfIgnoreCase((String) entry.getValue())));
    }
}
