package com.xiaohongshu.db.hercules.rdbms.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Map;

public class RDBMSDataTypeConverter implements DataTypeConverter<Integer, ResultSet> {

    @Override
    public BaseDataType convertElementType(Integer standard) {
        switch (standard) {
            case Types.NULL:
                return BaseDataType.NULL;
            case Types.BIT:
            case Types.TINYINT:
                return BaseDataType.BYTE;
            case Types.SMALLINT:
                return BaseDataType.SHORT;
            case Types.INTEGER:
                return BaseDataType.INTEGER;
            case Types.BIGINT:
                return BaseDataType.LONG;
            case Types.BOOLEAN:
                return BaseDataType.BOOLEAN;
            case Types.REAL:
            case Types.FLOAT:
                return BaseDataType.FLOAT;
            case Types.DOUBLE:
                return BaseDataType.DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BaseDataType.DECIMAL;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
                return BaseDataType.STRING;
            case Types.DATE:
                return BaseDataType.DATE;
            case Types.TIME:
                return BaseDataType.TIME;
            case Types.TIMESTAMP:
                return BaseDataType.DATETIME;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return BaseDataType.BYTES;
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
