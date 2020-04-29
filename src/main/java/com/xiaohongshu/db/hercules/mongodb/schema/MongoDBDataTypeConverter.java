package com.xiaohongshu.db.hercules.mongodb.schema;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class MongoDBDataTypeConverter implements DataTypeConverter<Object, Document> {
    @Override
    public DataType convertElementType(Object standard) {
        if (standard == null) {
            return DataType.NULL;
        } else if (standard instanceof Double) {
            return DataType.DOUBLE;
        } else if (standard instanceof Boolean) {
            return DataType.BOOLEAN;
        } else if (standard instanceof Date) {
            return DataType.DATE;
        } else if (standard instanceof Integer || standard instanceof Long) {
            return DataType.INTEGER;
        } else if (standard instanceof List) {
            return DataType.LIST;
        } else if (standard instanceof Document) {
            return DataType.MAP;
        } else {
            return DataType.STRING;
        }
    }

    @Override
    public Map<String, DataType> convertRowType(Document line) {
        return null;
    }
}
