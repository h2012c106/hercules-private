package com.xiaohongshu.db.hercules.rdbms.schema;

import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Map;

public class RDBMSDataTypeConverter implements DataTypeConverter<Integer, ResultSet> {

    @Override
    public DataType convertElementType(Integer standard) {
        switch (standard) {
            case Types.NULL:
                return DataType.NULL;
            case Types.BIT:
            case Types.TINYINT:
                return DataType.BYTE;
            case Types.SMALLINT:
                return DataType.SHORT;
            case Types.INTEGER:
                return DataType.INTEGER;
            case Types.BIGINT:
                return DataType.LONG;
            case Types.BOOLEAN:
                return DataType.BOOLEAN;
            case Types.REAL:
            case Types.FLOAT:
                return DataType.FLOAT;
            case Types.DOUBLE:
                return DataType.DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DataType.DECIMAL;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return DataType.STRING;
            case Types.DATE:
                return DataType.DATE;
            case Types.TIME:
                return DataType.TIME;
            case Types.TIMESTAMP:
                return DataType.DATETIME;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return DataType.BYTES;
            default:
                throw new SchemaException("Unsupported sql type, type code: " + standard);
        }
    }

    /**
     * 其实用不到
     *
     * @param line
     * @return
     */
    @Override
    public Map<String, DataType> convertRowType(ResultSet line) {
        throw new UnsupportedOperationException();
    }
}
