package com.xiaohongshu.db.hercules.mongodb.schema;

import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.types.Decimal128;

import java.util.*;

public class MongoDBDataTypeConverter implements DataTypeConverter<Object, Document> {

    private static final Log LOG = LogFactory.getLog(MongoDBDataTypeConverter.class);

    private final Set<Class> loggedType = new HashSet<>();

    /**
     * 别每读一行都来一个
     */
    private void logForceString(Class clazz) {
        if (!loggedType.contains(clazz)) {
            synchronized (this) {
                if (!loggedType.contains(clazz)) {
                    LOG.warn(String.format("Type [%s] will be treated as [%s].",
                            clazz.getCanonicalName(),
                            String.class.getCanonicalName()));
                    loggedType.add(clazz);
                }
            }
        }
    }

    @Override
    public DataType convertElementType(Object standard) {
        if (standard == null) {
            return DataType.NULL;
        } else if (standard instanceof Date) {
            return DataType.DATETIME;
        } else if (standard instanceof Byte) {
            return DataType.BYTE;
        } else if (standard instanceof Short) {
            return DataType.SHORT;
        } else if (standard instanceof Integer) {
            return DataType.INTEGER;
        } else if (standard instanceof Long) {
            return DataType.LONG;
        } else if (standard instanceof Boolean) {
            return DataType.BOOLEAN;
        } else if (standard instanceof Float) {
            return DataType.FLOAT;
        } else if (standard instanceof Double) {
            return DataType.DOUBLE;
        } else if (standard instanceof Decimal128) {
            return DataType.DECIMAL;
        } else if (standard instanceof List) {
            return DataType.LIST;
        } else if (standard instanceof Document) {
            return DataType.MAP;
        } else {
            if (!(standard instanceof String)) {
                logForceString(standard.getClass());
            }
            return DataType.STRING;
        }
    }

    @Override
    public Map<String, DataType> convertRowType(Document line) {
        throw new UnsupportedOperationException();
    }
}
