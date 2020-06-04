package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
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
                dt = BaseDataType.valueOf("SHORT");
                break;
            case "int":
                dt = BaseDataType.valueOf("INTEGER");
                break;
            case "long":
                dt = BaseDataType.valueOf("LONG");
                break;
            case "float":
                dt = BaseDataType.valueOf("FLOAT");
                break;
            case "double":
                dt = BaseDataType.valueOf("DOUBLE");
                break;
            case "decimal":
                dt = BaseDataType.valueOf("DECIMAL");
                break;
            case "char":
            case "varchar":
                dt = BaseDataType.valueOf("STRING");
                break;
            default:
                dt = BaseDataType.valueOf(type.toUpperCase());
        }
        return dt;
    }
}
