package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
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

    // 该函数用于处理从hive metastore中取出的为String的DataType
    public DataType hbaseConvertElementType(String type){
        DataType dt;
        switch(type.toLowerCase().split("\\(")[0]){
            case "short":
            case "tinyint":
            case "smallint":
                dt = DataType.valueOf("SHORT");
                break;
            case "int":
                dt = DataType.valueOf("INTEGER");
                break;
            case "long":
                dt = DataType.valueOf("LONG");
                break;
            case "float":
                dt = DataType.valueOf("FLOAT");
                break;
            case "double":
                dt = DataType.valueOf("DOUBLE");
                break;
            case "decimal":
                dt = DataType.valueOf("DECIMAL");
                break;
            case "char":
            case "varchar":
                dt = DataType.valueOf("STRING");
                break;
            default:
                dt = DataType.valueOf(type.toUpperCase());
        }
        return dt;
    }
}
