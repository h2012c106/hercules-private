package com.xiaohongshu.db.hercules.hbase.schema;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import lombok.Data;
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

    public DataType hbaseConvertElementType(String type){
        DataType dt;
        switch(type.toLowerCase().split("\\(")[0]){
            case "short":
            case "int":
            case "long":
            case "tinyint":
            case "smallint":
                dt = DataType.valueOf("INTEGER");
                break;
            case "float":
            case "double":
            case "decimal":
                dt = DataType.valueOf("DOUBLE");
                break;
            case "char":
            case "varchar":
                dt = DataType.valueOf("String");
                break;
            default:
                dt = DataType.valueOf(type.toUpperCase());
        }
        return dt;
    }
}
