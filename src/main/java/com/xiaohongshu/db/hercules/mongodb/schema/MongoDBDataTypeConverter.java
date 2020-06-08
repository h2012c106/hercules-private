package com.xiaohongshu.db.hercules.mongodb.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.mongodb.datatype.ObjectIdCustomDataType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class MongoDBDataTypeConverter implements DataTypeConverter<Object, Document> {

    @Override
    public DataType convertElementType(Object standard) {
        if (standard == null) {
            return BaseDataType.NULL;
        } else if (standard instanceof Date) {
            return BaseDataType.DATETIME;
        } else if (standard instanceof Byte) {
            return BaseDataType.BYTE;
        } else if (standard instanceof Short) {
            return BaseDataType.SHORT;
        } else if (standard instanceof Integer) {
            return BaseDataType.INTEGER;
        } else if (standard instanceof Long) {
            return BaseDataType.LONG;
        } else if (standard instanceof Boolean) {
            return BaseDataType.BOOLEAN;
        } else if (standard instanceof Float) {
            return BaseDataType.FLOAT;
        } else if (standard instanceof Double) {
            return BaseDataType.DOUBLE;
        } else if (standard instanceof Decimal128) {
            return BaseDataType.DECIMAL;
        } else if (standard instanceof List) {
            return BaseDataType.LIST;
        } else if (standard instanceof Document) {
            return BaseDataType.MAP;
        } else if (standard instanceof String) {
            return BaseDataType.STRING;
        } else if (standard instanceof ObjectId) {
            return ObjectIdCustomDataType.INSTANCE;
        } else if (standard instanceof Binary) {
            return BaseDataType.BYTES;
        } else {
            throw new RuntimeException("Unsupported mongo java class: " + standard.getClass().getCanonicalName());
        }
    }

    @Override
    public Map<String, DataType> convertRowType(Document line) {
        throw new UnsupportedOperationException();
    }
}
